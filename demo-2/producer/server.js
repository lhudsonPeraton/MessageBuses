import express from 'express'
import { Kafka, logLevel } from 'kafkajs'

const KAFKA_BROKER = process.env.KAFKA_BROKER ?? 'kafka:9092'
const TOPIC = 'jobs'
const PORT = 3000

const kafka = new Kafka({
  clientId: 'producer-webapp',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN,
})
const producer = kafka.producer()
await producer.connect()

const app = express()
app.use(express.json())
app.use(express.static('dist'))

app.post('/jobs', async (req, res) => {
  const { id, data } = req.body
  try {
    await producer.send({
      topic: TOPIC,
      messages: [{ key: String(id), value: JSON.stringify({ id, data }) }],
    })
    res.status(202).json({ id })
  } catch (err) {
    // No retry: same fire-and-forget contract as demo-1.
    res.status(500).json({ error: String(err) })
  }
})

app.listen(PORT, () => {
  console.log(`producer listening on ${PORT}, publishing to ${KAFKA_BROKER}/${TOPIC}`)
})
