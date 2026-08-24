// Stand-in for each service's `src/config/logger.ts` — `(msg, fields)` shape.
type Fields = Record<string, unknown>
const emit = (level: string, msg: string, fields?: Fields) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...fields }))
export const logger = {
  debug: (msg: string, fields?: Fields) => emit('debug', msg, fields),
  info: (msg: string, fields?: Fields) => emit('info', msg, fields),
  warn: (msg: string, fields?: Fields) => emit('warn', msg, fields),
  error: (msg: string, fields?: Fields) => emit('error', msg, fields),
}
