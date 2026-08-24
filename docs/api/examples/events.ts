// Example 5 — domain events: writers publish signals; instances fan out over SSE; a durable notifier gets retry/dead.
import { createConey } from '@lorvel/coney'
import { Events, NotifyOnTaskEvents, type DomainEvent } from './defs.js'
import { logger } from './logger.js'

declare const env: { RABBITMQ_URL: string }
declare const sse: { broadcast(projectId: string, payload: DomainEvent): void }
declare function sendNotification(event: DomainEvent, opts: { signal: AbortSignal }): Promise<void>

const mq = createConey({ url: env.RABBITMQ_URL, prefix: 'lorvel', name: 'backend-api', log: (e) => logger[e.level](e.event, e) })

// --- writer side (mcp-service / backend-api / embedding-worker) ------------------
export async function emitTaskUpdated(projectId: string, taskId: string, ref: string): Promise<void> {
  try {
    await mq.topic(Events).publish(
      'task.updated',
      { entity: 'task', action: 'updated', project_id: projectId, id: taskId, ref },
      { persistent: false }, // signal-only: skip the disk write, acceptable to lose on broker restart
    )
  } catch (err) {
    logger.warn('events.publish_failed', { taskId, error: err instanceof Error ? err.message : String(err) })
  }
}

// --- SSE fan-out: every backend-api instance gets every event (exclusive queue per instance) ----
export async function startEventFanout(): Promise<void> {
  const sub = await mq.topic(Events).subscribe(['task.*', 'ku.*', 'task_status_def.*'], (msg) => {
    sse.broadcast(msg.body.project_id, msg.body) // in order, one at a time (prefetch 1)
  })
  process.on('SIGTERM', () => void sub.cancel({ drainMs: 1_000 }))
}

// --- durable subscriber: competing consumers, retry 1 m then dead -------------------------------
export async function startNotifier(): Promise<void> {
  await mq.queue(NotifyOnTaskEvents).consume(
    async (msg) => {
      // msg.routingKey is 'task.updated' etc.; msg.body is a DomainEvent
      await sendNotification(msg.body, { signal: msg.signal })
    },
    { prefetch: 8, timeoutMs: 30_000 },
  )
}
