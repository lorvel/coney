// Block until RABBITMQ_URL accepts a connection (CI service containers boot
// slower than the job). Exits 1 after 120 s.
import amqplib from 'amqplib'

const url = process.env.RABBITMQ_URL
if (url === undefined) {
  console.error('RABBITMQ_URL is not set')
  process.exit(1)
}
const deadline = Date.now() + 120_000
for (;;) {
  try {
    const conn = await amqplib.connect(url)
    await conn.close()
    console.log('broker ready')
    process.exit(0)
  } catch (err) {
    if (Date.now() > deadline) {
      console.error(
        `broker not reachable within 120s: ${err instanceof Error ? err.message : String(err)}`,
      )
      process.exit(1)
    }
    await new Promise((resolve) => setTimeout(resolve, 2000))
  }
}
