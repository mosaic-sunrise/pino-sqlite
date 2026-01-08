import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createQueryHelper } from '../src/query.js'
import { initDatabase, insertBatch, closeDatabase } from '../src/db.js'
import { cleanup } from './test-utils.js'
import type { PinoLog } from '../src/types.js'

const TEST_DB = '/tmp/pino-sqlite-test.db'

async function runCleanup(): Promise<void> {
  await cleanup(TEST_DB)
}

describe('pino-sqlite transport', () => {
  beforeEach(runCleanup)
  afterEach(runCleanup)

  it('should write logs to SQLite database', () => {
    const db = initDatabase({ dbPath: TEST_DB })

    const logs: PinoLog[] = [
      { level: 30, time: Date.now(), msg: 'Test message', userId: 123 },
      { level: 50, time: Date.now(), msg: 'Error occurred', code: 'ERR_001' }
    ]

    insertBatch(db, 'logs', logs)
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)
    const results = query.find()

    expect(results).toHaveLength(2)
    expect(results[0].msg).toBe('Error occurred')
    expect(results[0].level).toBe('50-error')
    expect(results[1].msg).toBe('Test message')
    expect(results[1].level).toBe('30-info')
    expect(results[1].data.userId).toBe(123)

    query.close()
  })

  it('should batch multiple inserts in a transaction', () => {
    const db = initDatabase({ dbPath: TEST_DB })

    const logs: PinoLog[] = []
    for (let i = 0; i < 100; i++) {
      logs.push({ level: 30, time: Date.now() + i, msg: `Message ${i}` })
    }

    insertBatch(db, 'logs', logs)
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)
    const count = query.count()

    expect(count).toBe(100)

    query.close()
  })

  it('should store logger name as subsystem', () => {
    const db = initDatabase({ dbPath: TEST_DB })

    const logs: PinoLog[] = [
      { level: 30, time: Date.now(), msg: 'Database query', name: 'database' },
      { level: 30, time: Date.now(), msg: 'API request', name: 'api' }
    ]

    insertBatch(db, 'logs', logs)
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)

    const dbLogs = query.name('database').find()
    expect(dbLogs).toHaveLength(1)
    expect(dbLogs[0].msg).toBe('Database query')

    query.reset()
    const apiLogs = query.name('api').find()
    expect(apiLogs).toHaveLength(1)
    expect(apiLogs[0].msg).toBe('API request')

    query.close()
  })

  it('should create extracted field columns', () => {
    const db = initDatabase({
      dbPath: TEST_DB,
      extractFields: {
        'request_id': '$.requestId',
        'user_id': '$.userId'
      }
    })

    const logs: PinoLog[] = [
      { level: 30, time: Date.now(), msg: 'Request received', requestId: 'req-123', userId: 42 },
      { level: 30, time: Date.now(), msg: 'Another request', requestId: 'req-456', userId: 42 },
      { level: 30, time: Date.now(), msg: 'Different user', requestId: 'req-789', userId: 99 }
    ]

    insertBatch(db, 'logs', logs)
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)

    const userLogs = query.where('user_id', '42').find()
    expect(userLogs).toHaveLength(2)

    query.reset()
    const reqLogs = query.where('request_id', 'req-123').find()
    expect(reqLogs).toHaveLength(1)
    expect(reqLogs[0].msg).toBe('Request received')

    query.close()
  })

  it('should handle empty batches gracefully', () => {
    const db = initDatabase({ dbPath: TEST_DB })

    insertBatch(db, 'logs', [])
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)
    const count = query.count()

    expect(count).toBe(0)

    query.close()
  })

  it('should preserve all original log properties in data column', () => {
    const db = initDatabase({ dbPath: TEST_DB })

    const logs: PinoLog[] = [
      {
        level: 30,
        time: Date.now(),
        msg: 'Complex log',
        user: { id: 42, name: 'John' },
        metadata: { requestId: 'abc', traceId: '123' },
        tags: ['important', 'debug']
      }
    ]

    insertBatch(db, 'logs', logs)
    closeDatabase(db)

    const query = createQueryHelper(TEST_DB)
    const results = query.find()

    expect(results).toHaveLength(1)
    expect(results[0].data.user).toEqual({ id: 42, name: 'John' })
    expect(results[0].data.metadata).toEqual({ requestId: 'abc', traceId: '123' })
    expect(results[0].data.tags).toEqual(['important', 'debug'])

    query.close()
  })
})
