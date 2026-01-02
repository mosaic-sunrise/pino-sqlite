import type Database from 'better-sqlite3'

export interface TransportOptions {
  /** Path to the SQLite database file */
  dbPath: string
  /** Number of logs to batch before inserting (default: 100) */
  batchSize?: number
  /** Milliseconds between flush intervals (default: 1000) */
  flushIntervalMs?: number
  /** Custom table name (default: 'logs') */
  tableName?: string
  /**
   * Map of column names to JSON paths for field extraction.
   * Creates virtual generated columns with indexes for fast queries.
   * Example: { 'request_id': '$.requestId', 'user_id': '$.user.id' }
   */
  extractFields?: Record<string, string>
}

export interface PinoLog {
  level: number
  time: number
  msg?: string
  name?: string
  [key: string]: unknown
}

export interface LogEntry {
  id: number
  time: number
  level: number
  msg: string | null
  name: string | null
  data: string
  created_at: number
}

export interface ParsedLogEntry extends Omit<LogEntry, 'data'> {
  data: PinoLog
}

export type DatabaseInstance = Database.Database

export type ComparisonOperator = '=' | '!=' | '<' | '<=' | '>' | '>='
