import type { DatabaseInstance } from './types.js'

export function createSchema(
  db: DatabaseInstance,
  tableName: string,
  extractFields?: Record<string, string>
): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      time INTEGER NOT NULL,
      level INTEGER NOT NULL,
      msg TEXT,
      name TEXT,
      data JSON NOT NULL,
      created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
    )
  `)

  db.exec(`CREATE INDEX IF NOT EXISTS idx_${tableName}_time ON ${tableName}(time)`)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_${tableName}_level ON ${tableName}(level)`)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_${tableName}_name ON ${tableName}(name)`)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_${tableName}_time_level ON ${tableName}(time, level)`)

  if (extractFields) {
    for (const [columnName, jsonPath] of Object.entries(extractFields)) {
      addExtractedColumn(db, tableName, columnName, jsonPath)
    }
  }
}

function addExtractedColumn(
  db: DatabaseInstance,
  tableName: string,
  columnName: string,
  jsonPath: string
): void {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all() as Array<{ name: string }>
  const columnExists = columns.some((col) => col.name === columnName)

  if (!columnExists) {
    db.exec(`
      ALTER TABLE ${tableName}
      ADD COLUMN ${columnName} TEXT
      GENERATED ALWAYS AS (json_extract(data, '${jsonPath}')) VIRTUAL
    `)
    db.exec(`CREATE INDEX IF NOT EXISTS idx_${tableName}_${columnName} ON ${tableName}(${columnName})`)
  }
}
