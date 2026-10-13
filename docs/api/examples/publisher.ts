// Example 2 — backend-api / mcp-service: best-effort enqueue from a request handler.
import { createConey, ConeySchemaError } from 'coney'
import { EmbedKu } from './defs.js'
import { logger } from './logger.js'

declare const env: { RABBITMQ_URL: string }

export const mq = createConey({
  url: env.RABBITMQ_URL,
  prefix: 'lorvel',
  name: 'backend-api',
  log: (e) => logger[e.level](e.event, e),
  publishTimeoutMs: 2_000, // cap request latency during a broker blip; 5 s is the library default
})

// Connect in the background; a broker outage at boot is logged, never fatal —
// publishes are best-effort and `yarn backfill` is the safety net.
void mq.open().catch((err: unknown) =>
  logger.error('rabbitmq.open_failed', { error: err instanceof Error ? err.message : String(err) }),
)

/** Same contract as today: never throws, the KU write always wins. */
export async function publishEmbedJob(kuId: string): Promise<void> {
  try {
    await mq.queue(EmbedKu).publish({ kuId }) // resolves only when the broker confirmed
    logger.debug('embed.enqueued', { kuId })
  } catch (err) {
    if (err instanceof ConeySchemaError) throw err // a bug, not an outage — surface it
    logger.warn('embed.enqueue_failed', { kuId, error: err instanceof Error ? err.message : String(err) })
  }
}
