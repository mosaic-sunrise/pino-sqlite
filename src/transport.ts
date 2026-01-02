import build from 'pino-abstract-transport'
import { initDatabase, insertBatch, closeDatabase } from './db.js'
import type { DatabaseInstance, PinoLog, TransportOptions } from './types.js'

export default async function (opts: TransportOptions) {
  const {
    batchSize = 100,
    flushIntervalMs = 1000,
    tableName = 'logs'
  } = opts

  const db: DatabaseInstance = initDatabase(opts)
  let buffer: PinoLog[] = []
  let flushTimer: ReturnType<typeof setTimeout> | null = null

  const flush = (): void => {
    if (buffer.length > 0) {
      insertBatch(db, tableName, buffer)
      buffer = []
    }
  }

  const scheduleFlush = (): void => {
    if (flushTimer === null) {
      flushTimer = setTimeout(() => {
        flush()
        flushTimer = null
      }, flushIntervalMs)
    }
  }

  return build(
    async function (source) {
      for await (const obj of source) {
        buffer.push(obj as PinoLog)

        if (buffer.length >= batchSize) {
          flush()
        } else {
          scheduleFlush()
        }
      }
    },
    {
      close: (err, cb) => {
        if (flushTimer !== null) {
          clearTimeout(flushTimer)
        }
        flush()
        closeDatabase(db)
        cb(err)
      }
    }
  )
}
