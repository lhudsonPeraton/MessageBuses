import express from 'express'
import amqplib from 'amqplib'

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@rabbitmq/'
const QUEUE_NAME = 'jobs'
const PORT = 3000

const connection = await amqplib.connect(RABBITMQ_URL)
const channel = await connection.createChannel()
await channel.assertQueue(QUEUE_NAME, { durable: true })

const app = express()
app.use(express.json())
app.use(express.static('dist'))

app.post('/jobs', async (req, res) => {
  const { id, data } = req.body
  try {
    channel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify({ id, data })),
      { persistent: true }
    )
    res.status(202).json({ id })
  } catch (err) {
    res.status(500).json({ error: String(err) })
  }
})

app.listen(PORT, () => {
  console.log(`producer listening on ${PORT}, publishing to ${RABBITMQ_URL}/${QUEUE_NAME}`)
})
