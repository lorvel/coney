// Example 3 — one-off producer script: publish N messages, then close without losing the tail.
import { createConey } from '@lorvel/coney'
import { EmbedKu, EmbedTask } from './defs.js'
import { logger } from './logger.js'

declare const env: { RABBITMQ_URL: string }
declare function findStaleKuIds(): Promise<string[]>
declare function findStaleTaskIds(): Promise<string[]>

async function main(): Promise<void> {
  const mq = createConey({
    url: env.RABBITMQ_URL,
    prefix: 'lorvel',
    name: 'embedding-worker/backfill',
    log: (e) => logger[e.level](e.event, e),
    reconnect: { maxAttempts: 5 }, // a script should give up, not retry forever
    publishTimeoutMs: 30_000,
  })

  const [kuIds, taskIds] = await Promise.all([findStaleKuIds(), findStaleTaskIds()])
  logger.info('backfill.found', { kus: kuIds.length, tasks: taskIds.length })

  const kuQueue = mq.queue(EmbedKu)
  const taskQueue = mq.queue(EmbedTask)
  for (const id of kuIds) await kuQueue.publish({ kuId: id }) // each resolves on broker confirm
  for (const id of taskIds) await taskQueue.publish({ taskId: id })

  // No flush barrier needed: close() waits for outstanding confirms before closing.
  await mq.close()
  const depth = await Promise.all([kuQueue.inspect(), taskQueue.inspect()]).catch(() => undefined)
  logger.info('backfill.done', { published: kuIds.length + taskIds.length, depth })
}

main().catch((err: unknown) => {
  logger.error('backfill.fatal', { error: err instanceof Error ? err.message : String(err) })
  process.exit(1)
})
