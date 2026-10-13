// Example 1 — embedding-worker: two consumers, prefetch, handler timeout, graceful drain.
import { createConey, UnrecoverableError } from 'coney'
import { EmbedKu, EmbedTask } from './defs.js'
import { logger } from './logger.js'

declare const env: { RABBITMQ_URL: string; CONSUMER_PREFETCH: number }
declare function embedKu(kuId: string, opts: { signal: AbortSignal }): Promise<{ found: boolean }>
declare function embedTask(taskId: string, opts: { signal: AbortSignal }): Promise<{ found: boolean }>
declare const pool: { end(): Promise<void> }

const mq = createConey({
  url: env.RABBITMQ_URL,
  prefix: 'lorvel', // wire names: lorvel.ku.embed, lorvel.ku.embed.dead, lorvel.retry.10s …
  name: 'embedding-worker', // appId + consumer tags `embedding-worker.lorvel.ku.embed.1`
  log: (e) => logger[e.level](e.event, e), // one line: typed event → service logger
})

async function main(): Promise<void> {
  // Fail fast at boot (compose restarts us); afterwards reconnects are unbounded.
  await mq.open({ timeoutMs: 30_000 })

  await mq.queue(EmbedKu).consume(
    async (msg) => {
      // msg.body is { kuId: string } — already validated by EmbedKuJob
      const { found } = await embedKu(msg.body.kuId, { signal: msg.signal })
      if (!found) throw new UnrecoverableError(`ku ${msg.body.kuId} no longer exists`) // → dead, no retry
      logger.info('job.done', { kuId: msg.body.kuId, attempt: msg.attempt, redelivered: msg.redelivered })
      // resolve → ack · throw → retry after 10s / 1m / 5m → dead
    },
    { prefetch: env.CONSUMER_PREFETCH, timeoutMs: 120_000 },
  )

  await mq.queue(EmbedTask).consume(
    async (msg) => {
      const { found } = await embedTask(msg.body.taskId, { signal: msg.signal })
      if (!found) throw new UnrecoverableError(`task ${msg.body.taskId} no longer exists`)
    },
    { prefetch: env.CONSUMER_PREFETCH, timeoutMs: 120_000 },
  )

  logger.info('worker.ready', { queues: [mq.queue(EmbedKu).name, mq.queue(EmbedTask).name] })

  let shuttingDown = false
  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) return
    shuttingDown = true
    // compose stop_grace_period is 10 s: drain for 8 s, the rest is redelivered elsewhere.
    const { drained, inFlight } = await mq.close({ drainMs: 8_000 })
    logger.info('worker.shutdown', { signal, drained, inFlight })
    await pool.end()
    process.exit(0)
  }
  process.on('SIGTERM', () => void shutdown('SIGTERM'))
  process.on('SIGINT', () => void shutdown('SIGINT'))
}

main().catch((err: unknown) => {
  logger.error('worker.fatal', { error: err instanceof Error ? err.message : String(err) })
  process.exit(1)
})
