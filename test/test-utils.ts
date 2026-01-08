import { access, unlink } from 'node:fs/promises'

export async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

export async function cleanup(dbPath: string): Promise<void> {
  for (const file of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) {
    if (await fileExists(file)) {
      await unlink(file)
    }
  }
}
