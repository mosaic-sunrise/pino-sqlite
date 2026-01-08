import Database from 'better-sqlite3'
import { createSchema } from './schema.js'
import type { DatabaseInstance, PinoLog, TransportOptions } from './types.js'

/**
 * Convert Pino's numeric level to string format (e.g., 50 -> "50-error")
 */
export function levelToString(level: number): string {
  const levelMap: Record<number, string> = {
    10: '10-trace',
    20: '20-debug',
    30: '30-info',
    40: '40-warn',
    50: '50-error',
    60: '60-fatal'
  }
  return levelMap[level] ?? `${level}`
}

export function initDatabase(opts: TransportOptions): DatabaseInstance {
  const db = new Database(opts.dbPath)
  const tableName = opts.tableName ?? 'logs'

  db.pragma('journal_mode = WAL')

  createSchema(db, tableName, opts.extractFields)

  return db
}

export function insertBatch(
  db: DatabaseInstance,
  tableName: string,
  logs: PinoLog[]
): void {
  if (logs.length === 0) return

  const insert = db.prepare(`
    INSERT INTO ${tableName} (time, level, msg, name, data)
    VALUES (?, ?, ?, ?, ?)
  `)

  const insertMany = db.transaction((items: PinoLog[]) => {
    for (const log of items) {
      insert.run(
        log.time,
        levelToString(log.level),
        log.msg ?? null,
        log.name ?? null,
        JSON.stringify(log)
      )
    }
  })

  insertMany(logs)
}

export function closeDatabase(db: DatabaseInstance): void {
  db.close()
}
