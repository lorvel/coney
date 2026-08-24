// Shared catalog — pure values, no client, no I/O (per service today; a shared package later).
import { z } from 'zod'
import { defineQueue, defineTopic } from '@lorvel/coney'

export const EmbedKuJob = z.object({ kuId: z.string().uuid() })
export const EmbedTaskJob = z.object({ taskId: z.string().uuid() })

/** Domain event signal — ids only, receivers re-read via REST. */
export const DomainEvent = z.object({
  entity: z.enum(['task', 'ku', 'task_status_def', 'project']),
  action: z.enum(['created', 'updated', 'deleted', 'embedded']),
  project_id: z.string().uuid(),
  id: z.string().uuid(),
  ref: z.string().optional(),
  parent_id: z.string().uuid().nullable().optional(),
})
export type DomainEvent = z.infer<typeof DomainEvent>

export const EmbedKu = defineQueue({
  name: 'ku.embed',
  schema: EmbedKuJob,
  retry: { delays: [10_000, 60_000, 300_000] }, // 3 retries → 4 attempts, then ku.embed.dead
})

export const EmbedTask = defineQueue({
  name: 'task.embed',
  schema: EmbedTaskJob, // no `retry` → client default ladder
})

export const Events = defineTopic({ name: 'events', schema: DomainEvent })

/** Durable subscriber: competing consumers, full retry/dead — e.g. a notifier. */
export const NotifyOnTaskEvents = defineQueue({
  name: 'events.notify',
  schema: DomainEvent,
  retry: { delays: [60_000] },
  bindings: [{ topic: Events, patterns: ['task.*'] }],
})
