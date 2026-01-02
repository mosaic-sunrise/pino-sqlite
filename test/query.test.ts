import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { unlink, access } from 'node:fs/promises'
import Database from 'better-sqlite3'
import { createQueryHelper, LogQuery } from '../src/query.js'
import { createSchema } from '../src/schema.js'
import { insertBatch } from '../src/db.js'
import type { PinoLog } from '../src/types.js'

const TEST_DB = '/tmp/pino-sqlite-query-test.db'

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

async function cleanup(): Promise<void> {
  for (const file of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await fileExists(file)) {
      await unlink(file)
    }
  }
}

function createTestLogs(): PinoLog[] {
  const now = Date.now()
  return [
    { level: 30, time: now - 3600000, msg: 'Old info message', name: 'api' },
    { level: 30, time: now - 1800000, msg: 'Info message', name: 'database', query: 'SELECT * FROM users' },
    { level: 40, time: now - 900000, msg: 'Warning message', name: 'api', userId: 42 },
    { level: 50, time: now - 600000, msg: 'Error: Connection failed', name: 'database', errorCode: 'CONN_ERR' },
    { level: 50, time: now - 300000, msg: 'Error: Query timeout', name: 'database', query: 'SELECT * FROM orders' },
    { level: 30, time: now, msg: 'Recent info', name: 'api', userId: 99 }
  ]
}

describe('LogQuery', () => {
  beforeAll(async () => {
    await cleanup()
    const db = new Database(TEST_DB)
    db.pragma('journal_mode = WAL')
    createSchema(db, 'logs', { user_id: '$.userId' })
    insertBatch(db, 'logs', createTestLogs())
    db.close()
  })

  afterAll(cleanup)

  it('should find all logs', () => {
    const query = createQueryHelper(TEST_DB)
    const logs = query.find()
    expect(logs).toHaveLength(6)
    query.close()
  })

  it('should filter by name', () => {
    const query = createQueryHelper(TEST_DB)
    const logs = query.name('database').find()
    expect(logs).toHaveLength(3)
    expect(logs.every((l) => l.name === 'database')).toBe(true)
    query.close()
  })

  it('should filter by level', () => {
    const query = createQueryHelper(TEST_DB)

    const errors = query.level(50, '=').find()
    expect(errors).toHaveLength(2)

    query.reset()
    const warningsAndAbove = query.level(40).find()
    expect(warningsAndAbove).toHaveLength(3)

    query.close()
  })

  it('should filter by time range', () => {
    const query = createQueryHelper(TEST_DB)
    const now = Date.now()

    const recentLogs = query.timeRange(now - 1000000).find()
    expect(recentLogs.length).toBeGreaterThan(0)
    expect(recentLogs.length).toBeLessThan(6)

    query.close()
  })

  it('should filter using since()', () => {
    const query = createQueryHelper(TEST_DB)

    const lastHour = query.since(3600000).find()
    expect(lastHour.length).toBeGreaterThanOrEqual(5)

    query.close()
  })

  it('should search message content', () => {
    const query = createQueryHelper(TEST_DB)

    const errorLogs = query.messageContains('Error').find()
    expect(errorLogs).toHaveLength(2)
    expect(errorLogs.every((l) => l.msg?.includes('Error'))).toBe(true)

    query.close()
  })

  it('should query JSON properties', () => {
    const query = createQueryHelper(TEST_DB)

    const queryLogs = query.where('$.query', 'SELECT * FROM users').find()
    expect(queryLogs).toHaveLength(1)
    expect(queryLogs[0].data.query).toBe('SELECT * FROM users')

    query.close()
  })

  it('should query extracted columns', () => {
    const query = createQueryHelper(TEST_DB)

    const userLogs = query.where('user_id', '42').find()
    expect(userLogs).toHaveLength(1)
    expect(userLogs[0].data.userId).toBe(42)

    query.close()
  })

  it('should check property existence with has()', () => {
    const query = createQueryHelper(TEST_DB)

    const logsWithUserId = query.has('$.userId').find()
    expect(logsWithUserId).toHaveLength(2)

    query.close()
  })

  it('should apply limit and offset', () => {
    const query = createQueryHelper(TEST_DB)

    const limited = query.limit(2).find()
    expect(limited).toHaveLength(2)

    query.reset()
    const offset = query.limit(2).offset(2).find()
    expect(offset).toHaveLength(2)
    expect(offset[0].id).not.toBe(limited[0].id)

    query.close()
  })

  it('should count matching logs', () => {
    const query = createQueryHelper(TEST_DB)

    const total = query.count()
    expect(total).toBe(6)

    query.reset()
    const errorCount = query.level(50, '=').count()
    expect(errorCount).toBe(2)

    query.close()
  })

  it('should get distinct values', () => {
    const query = createQueryHelper(TEST_DB)

    const names = query.distinct('name')
    expect(names).toContain('api')
    expect(names).toContain('database')
    expect(names).toHaveLength(2)

    query.close()
  })

  it('should combine multiple filters', () => {
    const query = createQueryHelper(TEST_DB)

    const logs = query
      .name('database')
      .level(50, '=')
      .messageContains('timeout')
      .find()

    expect(logs).toHaveLength(1)
    expect(logs[0].msg).toBe('Error: Query timeout')

    query.close()
  })

  it('should order results', () => {
    const query = createQueryHelper(TEST_DB)

    const ascending = query.orderBy('time', 'ASC').find()
    expect(ascending[0].time).toBeLessThan(ascending[5].time)

    query.reset()
    const descending = query.orderBy('time', 'DESC').find()
    expect(descending[0].time).toBeGreaterThan(descending[5].time)

    query.close()
  })
})
