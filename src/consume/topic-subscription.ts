/**
 * Subscriptions: per-instance listeners of a topic on a server-named exclusive
 * queue (spec §4). The library acks after the handler settles; a failure is
 * dropped (`coney.subscription_error`) — durable delivery is a queue def with
 * `bindings`. A reconnect declares a NEW queue: messages in between are lost
 * (`coney.subscription_reset`).
 */

import type { Channel, ConsumeMessage } from 'amqplib'
import { decodeJson } from '../core/codec.js'
import { reasonOf, type LogEmitter } from '../core/log.js'
import type { ChannelFactory } from '../connection/manager.js'
import type { TopicDef } from '../defs/define.js'
import type { Handler, SubscribeOptions } from './types.js'
import { runConsumePipeline, truncateReason } from './pipeline.js'
import { contextOf } from './delivery.js'
import { BaseConsumer, type ConsumerHooks } from './runtime.js'

interface TopicSubscriptionArgs<T> {
  def: TopicDef<T>
  /** Wire name of the topic exchange. */
  topicWire: string
  patterns: readonly string[]
  handler: Handler<T>
  options: SubscribeOptions | undefined
  consumerTag: string
  emit: LogEmitter
  hooks: ConsumerHooks
}

export class TopicSubscription<T> extends BaseConsumer {
  /** Server-named queue of the CURRENT connection. */
  private queueName: string | null = null
  private everAttached = false

  constructor(private readonly args: TopicSubscriptionArgs<T>) {
    super(args.consumerTag, args.options?.prefetch ?? 1, args.emit, args.hooks)
  }

  protected async doAttach(factory: ChannelFactory): Promise<void> {
    // A plain channel: subscriptions never publish, so no confirms needed.
    const channel = await factory.createChannel()
    try {
      await channel.prefetch(this.prefetchCount)
      const declared = await channel.assertQueue('', { exclusive: true, autoDelete: true })
      for (const pattern of this.args.patterns) {
        await channel.bindQueue(declared.queue, this.args.topicWire, pattern)
      }
      // Adopt BEFORE consume — see BaseConsumer.onDelivery on the delivery race.
      this.adoptChannel(channel)
      this.queueName = declared.queue
      const ok = await channel.consume(declared.queue, (raw) => this.onDelivery(channel, raw), {
        consumerTag: this.tag,
      })
      this.consumerTag = ok.consumerTag
      if (this.everAttached) {
        this.emit({
          event: 'coney.subscription_reset',
          level: 'warn',
          topic: this.args.topicWire,
          patterns: [...this.args.patterns],
        })
      } else {
        this.emit({
          event: 'coney.consumer_started',
          level: 'info',
          queue: declared.queue,
          consumerTag: ok.consumerTag,
          prefetch: this.prefetchCount,
        })
      }
      this.everAttached = true
    } catch (err) {
      await channel.close().catch(() => {})
      throw err
    }
  }

  /** The exclusive queue of the CURRENT attach — a fresh one is declared by each. */
  get queueLabel(): string {
    return this.queueName ?? ''
  }

  protected async processDelivery(channel: Channel, raw: ConsumeMessage): Promise<void> {
    const context = contextOf(raw)
    let body: unknown
    let decodeError: unknown = null
    try {
      body = decodeJson(raw.content)
    } catch (err) {
      decodeError = err
    }
    if (decodeError !== null) {
      this.emit({
        event: 'coney.subscription_error',
        level: 'error',
        topic: this.args.topicWire,
        messageId: context.id,
        reason: truncateReason(`decode: ${reasonOf(decodeError)}`),
      })
      this.ack(channel, raw)
      return
    }
    const controller = new AbortController()
    this.controllers.add(controller)
    let outcome
    try {
      outcome = await runConsumePipeline({
        schema: this.args.def.schema,
        body,
        context,
        handler: this.args.handler,
        controller,
        timeoutMs: this.args.options?.timeoutMs,
        delays: null, // subscription mode: any failure → drop
        onTimeout: (timeoutMs) =>
          this.emit({
            event: 'coney.handler_timeout',
            level: 'warn',
            queue: this.queueName ?? '',
            messageId: context.id,
            timeoutMs,
          }),
        onLate: (late) =>
          this.emit({
            event: 'coney.handler_late',
            level: 'warn',
            queue: this.queueName ?? '',
            messageId: context.id,
            outcome: late,
          }),
      })
    } finally {
      this.controllers.delete(controller)
    }
    if (outcome.kind === 'expired') {
      this.emit({
        event: 'coney.message_expired',
        level: 'warn',
        queue: this.queueName ?? '',
        messageId: context.id,
        attempt: context.attempt,
        expiresAt: outcome.expiresAt.toISOString(),
      })
    }
    if (outcome.kind === 'drop') {
      this.emit({
        event: 'coney.subscription_error',
        level: 'error',
        topic: this.args.topicWire,
        messageId: context.id,
        reason: outcome.reason,
      })
    }
    // Ack, drop and expired all acknowledge — subscriptions never requeue or retry.
    this.ack(channel, raw)
  }

  private ack(channel: Channel, raw: ConsumeMessage): void {
    const messageId = typeof raw.properties.messageId === 'string' ? raw.properties.messageId : ''
    this.safeAck(channel, raw, this.queueName ?? '', messageId)
  }
}
