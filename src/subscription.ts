/**
 * Subscriptions: per-instance listeners of a topic on a server-named exclusive
 * queue (spec §4). The library acks after the handler settles; a failure is
 * dropped (`coney.subscription_error`) — durable delivery is a queue def with
 * `bindings`. A reconnect declares a NEW queue: messages in between are lost
 * (`coney.subscription_reset`).
 */

import type { Channel, ConsumeMessage } from 'amqplib'
// Type-only: no runtime cycle (client.ts imports the runtimes as values).
import type { CloseResult } from './client.js'
import type { TopicDef } from './define.js'
import type { LogEmitter } from './log.js'
import {
  BaseConsumer,
  contextOf,
  reasonOf,
  runConsumePipeline,
  truncateReason,
  type ChannelFactory,
  type Handler,
} from './consumer.js'

export interface SubscribeOptions {
  /** Max messages in flight for this subscription. Default `1` (in-order processing). */
  readonly prefetch?: number
  /** Same semantics as {@link ConsumeOptions.timeoutMs}, except the outcome is "drop" (subscriptions have no retry). */
  readonly timeoutMs?: number
}

export interface CancelOptions {
  /** Max wait for in-flight handlers. Default `10_000` ms. */
  readonly drainMs?: number
}

export interface Subscription {
  /** `false` once cancelled or after the client closed. */
  readonly active: boolean
  /** Stop receiving (survives reconnects until called). Resolves after in-flight handlers settle or `drainMs` passes. Idempotent. */
  cancel(options?: CancelOptions): Promise<CloseResult>
}

interface TopicSubscriptionArgs<T> {
  def: TopicDef<T>
  /** Wire name of the topic exchange. */
  topicWire: string
  patterns: readonly string[]
  handler: Handler<T>
  options: SubscribeOptions | undefined
  consumerTag: string
  emit: LogEmitter
  onDispose: (runtime: BaseConsumer) => void
}

export class TopicSubscription<T> extends BaseConsumer {
  /** Server-named queue of the CURRENT connection. */
  private queueName: string | null = null
  private everAttached = false

  constructor(private readonly args: TopicSubscriptionArgs<T>) {
    super(args.consumerTag, args.options?.prefetch ?? 1, args.emit, args.onDispose)
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
      const ok = await channel.consume(declared.queue, (raw) => this.onDelivery(channel, raw), { consumerTag: this.tag })
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

  protected handleBrokerCancel(): void {
    // The exclusive queue vanished (its channel/connection is on the way down);
    // the next `setup` declares a fresh one.
    const { channel } = this
    this.channel = null
    this.consumerTag = null
    this.emit({ event: 'coney.consumer_stopped', level: 'info', queue: this.queueName ?? '', consumerTag: this.tag })
    if (channel !== null) void channel.close().catch(() => {})
  }

  protected async processDelivery(channel: Channel, raw: ConsumeMessage): Promise<void> {
    const context = contextOf(raw)
    let body: unknown
    let decodeError: unknown = null
    try {
      body = JSON.parse(raw.content.toString('utf8'))
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
    if (outcome.kind === 'drop') {
      this.emit({
        event: 'coney.subscription_error',
        level: 'error',
        topic: this.args.topicWire,
        messageId: context.id,
        reason: outcome.reason,
      })
    }
    // Both ack and drop acknowledge — subscriptions never requeue or retry.
    this.ack(channel, raw)
  }

  private ack(channel: Channel, raw: ConsumeMessage): void {
    try {
      channel.ack(raw)
    } catch (err) {
      this.emit({
        event: 'coney.ack_failed',
        level: 'warn',
        queue: this.queueName ?? '',
        messageId: typeof raw.properties.messageId === 'string' ? raw.properties.messageId : '',
        reason: reasonOf(err),
      })
    }
  }
}
