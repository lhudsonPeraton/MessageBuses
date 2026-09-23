import { useRef, useState } from 'react'

const SEND_INTERVAL_MS = 200
const BATCH_SIZES = [10, 100, 500, 1000]

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

export default function App() {
  const [jobs, setJobs] = useState([])
  const [batchSize, setBatchSize] = useState(BATCH_SIZES[0])
  const [sending, setSending] = useState(false)
  const nextId = useRef(1)

  const setStatus = (id, status) => {
    setJobs((prev) => prev.map((j) => (j.id === id ? { ...j, status } : j)))
  }

  const sendJobs = async () => {
    const batch = Array.from({ length: batchSize }, () => ({
      id: nextId.current++,
      data: Math.round(Math.random() * 10000) / 10000,
      status: 'not sent',
    }))
    setJobs((prev) => [...prev, ...batch])
    setSending(true)

    for (const job of batch) {
      setStatus(job.id, 'sending')
      // Fire-and-forget: no retry, errors ignored. The backend publishes the
      // job to the Kafka topic; a stopped consumer no longer means job loss.
      fetch('/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: job.id, data: job.data }),
      }).catch(() => {})
      setStatus(job.id, 'sent')
      await sleep(SEND_INTERVAL_MS)
    }

    setSending(false)
  }

  const reset = () => {
    setJobs([])
    nextId.current = 1
  }

  return (
    <main>
      <h1>Producer (w/ Kafka)</h1>
      <div className="controls">
        <label>
          Send jobs:{' '}
          <select
            value={batchSize}
            disabled={sending}
            onChange={(e) => setBatchSize(Number(e.target.value))}
          >
            {BATCH_SIZES.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <button onClick={sendJobs} disabled={sending}>
          {sending ? 'Sending…' : 'Send'}
        </button>
        <button onClick={reset} disabled={sending || jobs.length === 0}>
          Reset
        </button>
      </div>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Data</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id}>
              <td>{job.id}</td>
              <td>{job.data}</td>
              <td className={`status status-${job.status.replace(' ', '-')}`}>
                {job.status}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  )
}
