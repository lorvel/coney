/**
 * The channel-per-consumer runtime for work queues (spec §4). Iron rule: a
 * retry/dead publish is CONFIRMED before the original delivery is acked — a
 * crash in between yields a duplicate, never a loss.
 */

import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib'
import { decodeJson } from '../core/codec.js'
import { reasonOf, type LogEmitter } from '../core/log.js'
import type { ChannelFactory } from '../connection/manager.js'
import type { QueueDef } from '../defs/define.js'
import type { ConsumeOptions, Handler } from './types.js'
import {
  runConsumePipeline,
  truncateReason,
  type DeliveryContext,
  type PipelineOutcome,
} from './pipeline.js'
import { confirmPublish, contextOf, republishOptions } from './delivery.js'
import { BaseConsumer, type ConsumerHooks } from './runtime.js'

interface QueueConsumerArgs<T> {
  def: QueueDef<T>
  /** Wire name of the consumed queue — also the routing key of every retry publish. */
  wire: string
  handler: Handler<T>
  options: ConsumeOptions | undefined
  /** Effective ladder (def override or client default). */
  delays: readonly number[]
  /** Wire name of the retry bucket for a delay. */
  bucketWire: (delayMs: number) => string
  consumerTag: string
  emit: LogEmitter
  hooks: ConsumerHooks
}

export class QueueConsumer<T> extends BaseConsumer {
  constructor(private readonly args: QueueConsumerArgs<T>) {
    super(args.consumerTag, args.options?.prefetch ?? 1, args.emit, args.hooks)
  }

  protected async doAttach(factory: ChannelFactory): Promise<void> {
    const channel = await factory.createConfirmChannel()
    try {
      await channel.prefetch(this.prefetchCount)
      // Adopt BEFORE consume: deliveries may arrive the moment the broker sees
      // the consumer (a failed consume un-adopts through the close listener).
      this.adoptChannel(channel)
      const ok = await channel.consume(this.args.wire, (raw) => this.onDelivery(channel, raw), {
        consumerTag: this.tag,
      })
      this.consumerTag = ok.consumerTag
      this.emit({
        event: 'coney.consumer_started',
        level: 'info',
        queue: this.args.wire,
        consumerTag: ok.consumerTag,
        prefetch: this.prefetchCount,
      })
    } catch (err) {
      await channel.close().catch(() => {}) // a failed consume (404) already killed it
      throw err
    }
  }

  get queueLabel(): string {
    return this.args.wire
  }

  protected async processDelivery(
    channel: Channel | ConfirmChannel,
    raw: ConsumeMessage,
  ): Promise<void> {
    const context = contextOf(raw)
    const outcome = await this.runDelivery(raw, context)
    await this.settleOutcome(channel as ConfirmChannel, raw, context, outcome)
  }

  private async runDelivery(
    raw: ConsumeMessage,
    context: DeliveryContext,
  ): Promise<PipelineOutcome> {
    let body: unknown
    try {
      body = decodeJson(raw.content)
    } catch (err) {
      return {
        kind: 'dead',
        attempt: context.attempt,
        reason: truncateReason(`decode: ${reasonOf(err)}`),
        error: err,
      }
    }
    const controller = new AbortController()
    this.controllers.add(controller)
    try {
      return await runConsumePipeline({
        schema: this.args.def.schema,
        body,
        context,
        handler: this.args.handler,
        controller,
        timeoutMs: this.args.options?.timeoutMs,
        delays: this.args.delays,
        onTimeout: (timeoutMs) =>
          this.emit({
            event: 'coney.handler_timeout',
            level: 'warn',
            queue: this.args.wire,
            messageId: context.id,
            timeoutMs,
          }),
        onLate: (late) =>
          this.emit({
            event: 'coney.handler_late',
            level: 'warn',
            queue: this.args.wire,
            messageId: context.id,
            outcome: late,
          }),
      })
    } finally {
      this.controllers.delete(controller)
    }
  }

  private async settleOutcome(
    channel: ConfirmChannel,
    raw: ConsumeMessage,
    context: DeliveryContext,
    outcome: PipelineOutcome,
  ): Promise<void> {
    if (outcome.kind === 'ack') {
      this.ack(channel, raw, context)
      return
    }
    if (outcome.kind === 'retry') {
      const confirmed = await this.publishInternal(channel, {
        exchange: this.args.bucketWire(outcome.delayMs),
        // The queue's own wire name — NEVER fields.routingKey (a bound queue's key
        // is the topic key; the bucket's DLX delivers by the key it was published with).
        routingKey: this.args.wire,
        content: raw.content,
        options: republishOptions(raw.properties, {
          ...context.headers,
          'x-retries': outcome.attempt,
          'x-retry-at': new Date(Date.now() + outcome.delayMs).toISOString(),
        }),
      })
      if (confirmed) {
        this.ack(channel, raw, context)
        this.emit({
          event: 'coney.message_retried',
          level: 'warn',
          queue: this.args.wire,
          messageId: context.id,
          attempt: outcome.attempt,
          delayMs: outcome.delayMs,
          reason: outcome.reason,
        })
      }
      return
    }
    if (outcome.kind === 'dead') {
      const confirmed = await this.publishInternal(channel, {
        exchange: '',
        routingKey: `${this.args.wire}.dead`,
        content: raw.content,
        options: republishOptions(raw.properties, {
          ...context.headers,
          'x-reason': outcome.reason,
          'x-failed-at': new Date().toISOString(),
        }),
      })
      if (confirmed) {
        this.ack(channel, raw, context)
        this.emit({
          event: 'coney.message_dead',
          level: 'error',
          queue: this.args.wire,
          messageId: context.id,
          attempt: outcome.attempt,
          reason: outcome.reason,
        })
      }
      return
    }
    // 'drop' cannot happen in queue mode (delays is never null here).
  }

  /**
   * Publish retry/dead on the consuming channel and wait for the broker confirm.
   * `false` = the channel died mid-flight: the original stays unacked and the
   * broker redelivers it with the SAME attempt — the "free retry" (spec §4).
   */
  private async publishInternal(
    channel: ConfirmChannel,
    args: {
      exchange: string
      routingKey: string
      content: Buffer
      options: Record<string, unknown>
    },
  ): Promise<boolean> {
    try {
      await confirmPublish(channel, args)
      return true
    } catch (err) {
      this.emit({
        event: 'coney.publish_failed',
        level: 'warn',
        target: args.exchange === '' ? args.routingKey : args.exchange,
        routingKey: args.routingKey,
        reason: reasonOf(err),
      })
      return false
    }
  }

  private ack(channel: ConfirmChannel, raw: ConsumeMessage, context: DeliveryContext): void {
    this.safeAck(channel, raw, this.args.wire, context.id)
  }
}
