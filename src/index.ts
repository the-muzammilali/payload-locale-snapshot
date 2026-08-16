/**
 * localeSnapshot — Enterprise-Grade PostgreSQL Locale Preservation Plugin for Payload CMS 3.x
 *
 * PROBLEM:
 * In Payload CMS 3.x with the PostgreSQL adapter (@payloadcms/db-postgres), saving a document
 * in one locale causes Payload's Drizzle adapter to execute a CASCADE DELETE on all block & array
 * tables where "_parent_id" = <documentId> without filtering by "_locale". Payload then only
 * re-inserts the blocks for the currently active locale, resulting in silent data loss of all
 * other translations.
 *
 * ARCHITECTURAL SOLUTION:
 * 1. `beforeChange` hook:
 *    - Traverses foreign key constraints in topological hierarchy order (Level 1 -> Level 2 -> Level 3...).
 *    - Dynamically snapshots all block & array rows for non-active locales into `req._localeSnapshot`.
 *    - Supports integer, serial, text, and UUID primary keys with universal `::text` casting.
 *    - Acquires both in-memory and PostgreSQL Distributed Advisory Locks (`pg_advisory_lock`) with
 *      an automatic safety timeout to prevent connection leaks if a save is aborted.
 * 2. `afterChange` hook:
 *    - Default (VPS/Persistent Node.js): Non-blocking post-commit execution via `setImmediate`
 *      with automatic lock-contention retry loop.
 *    - Serverless Mode (`serverless: true`): Direct synchronous await within the hook lifecycle.
 *    - Atomically restores snapshotted rows inside a dedicated BEGIN/COMMIT/ROLLBACK PostgreSQL
 *      transaction on a dedicated PoolClient.
 *    - Durable fail-safe recovery: In the rare event of a database crash during restore, writes
 *      the snapshot to a recovery JSON file for instant zero-loss restoration.
 *
 * KEY GUARANTEES:
 * - Universal ID Support: Compatible with both integer/serial and UUID (`idType: 'uuid'`) schemas.
 * - Distributed Concurrency Safe: PostgreSQL native advisory locking (`pg_advisory_lock`).
 * - Leak-Proof Safety Timeout: Locks & connections auto-release if Payload aborts before afterChange.
 * - Topological FK Ordering: Parent block tables restored before child tables.
 * - Atomic Transactional Restoration: Full BEGIN/COMMIT/ROLLBACK transaction per restore.
 * - Serverless-Ready: Configurable `serverless: true` synchronous restoration mode.
 * - Pure Schema-Driven Auto-Discovery: Works on any collection or global without hardcoded slugs.
 * - Zero API Costs / Pure SQL: Zero external dependencies, reuses native Payload connection pool.
 */

import type { Config, Plugin, CollectionConfig, GlobalConfig, Field } from 'payload'
import pg from 'pg'
import fs from 'node:fs'
import path from 'node:path'

const { Pool } = pg

// ─── Types & Interfaces ───────────────────────────────────────────────────────

export interface LocaleSnapshotOptions {
  /**
   * Explicit collection slugs to guard.
   * If omitted, collections with localized blocks or arrays are auto-detected.
   */
  collections?: string[]

  /**
   * Explicit global slugs to guard.
   * If omitted, globals with localized blocks or arrays are auto-detected.
   */
  globals?: string[]

  /**
   * Enable verbose logging in server console. Defaults to true.
   */
  debug?: boolean

  /**
   * When true, a snapshot failure in beforeChange will throw an error, aborting the save.
   * When false (default), the error is logged and the save proceeds without locale protection.
   */
  failOnError?: boolean

  /**
   * Maximum depth for foreign key hierarchy traversal (default: 10).
   */
  maxDepth?: number

  /**
   * Time-to-live for the schema cache in milliseconds (default: 300,000 / 5 minutes).
   * Set to 0 to disable caching entirely.
   */
  schemaCacheTTL?: number

  /**
   * Timeout in milliseconds for distributed advisory locks (default: 30,000 / 30s).
   */
  lockTimeoutMs?: number

  /**
   * Set to true for serverless runtimes (AWS Lambda, Vercel, Cloudflare Workers)
   * where background event loop ticks (`setImmediate`) may be throttled or frozen.
   * When true, restoration is awaited synchronously in afterChange. Defaults to false.
   */
  serverless?: boolean
}

/** Alias for backward compatibility */
export type LocaleGuardOptions = LocaleSnapshotOptions

export interface RowData {
  [key: string]: unknown
}

export interface TableSnapshot {
  table: string
  rows: RowData[]
}

export interface LevelSnapshot {
  level: number
  tables: TableSnapshot[]
}

export interface LocaleSnapshot {
  docId: number | string
  rootTable: string
  levels: LevelSnapshot[]
  totalRows: number
  timestamp: number
}

// Augment PayloadRequest with _localeSnapshot & lock release
declare module 'payload' {
  interface PayloadRequest {
    _localeSnapshot?: LocaleSnapshot
    _localeSnapshotReleaseLock?: () => Promise<void>
  }
}

// ─── Constants ───────────────────────────────────────────────────────────────

const DEFAULT_MAX_DEPTH = 10
const DEFAULT_SCHEMA_CACHE_TTL = 5 * 60 * 1000 // 5 minutes
const DEFAULT_LOCK_TIMEOUT_MS = 30 * 1000 // 30 seconds
const CHUNK_PARAM_BUDGET = 5000

// ─── Database Pool Management ────────────────────────────────────────────────

let _fallbackPool: pg.Pool | null = null

function detectSsl(connStr: string): boolean {
  if (!connStr) return false
  try {
    const url = new URL(connStr)
    const sslmode = url.searchParams.get('sslmode')
    if (sslmode) {
      const sslEnabled = ['require', 'verify-ca', 'verify-full', 'prefer']
      return sslEnabled.includes(sslmode)
    }
    const sslParam = url.searchParams.get('ssl')
    if (sslParam === 'true' || sslParam === '1') return true
  } catch {
    // URL parsing fallback
  }
  return (
    connStr.includes('sslmode=require') ||
    connStr.includes('ssl=true') ||
    process.env.NODE_ENV === 'production'
  )
}

function getDatabasePool(req: any, incomingConfig?: Config): pg.Pool {
  // 1. Prefer Payload's native database pool
  if (req?.payload?.db?.pool) {
    return req.payload.db.pool as pg.Pool
  }

  // 2. Fallback to cached singleton pool
  if (!_fallbackPool) {
    const connStr =
      (incomingConfig?.db as any)?.pool?.connectionString ||
      process.env.DATABASE_URL ||
      ''

    const useSsl = detectSsl(connStr)

    _fallbackPool = new Pool({
      connectionString: connStr,
      ssl: useSsl ? { rejectUnauthorized: false } : false,
      max: 10,
    })
  }

  return _fallbackPool
}

// ─── Schema Hierarchy Discovery (TTL-Cached) ─────────────────────────────────

interface SchemaHierarchy {
  parentToChildren: Map<string, string[]>
  tablesWithLocale: Set<string>
}

let _schemaCache: SchemaHierarchy | null = null
let _schemaCacheTimestamp = 0

/**
 * Manually invalidate the schema cache (useful after running database migrations).
 */
export function resetSchemaCache(): void {
  _schemaCache = null
  _schemaCacheTimestamp = 0
}

async function getSchemaHierarchy(
  pool: pg.Pool,
  ttl: number = DEFAULT_SCHEMA_CACHE_TTL
): Promise<SchemaHierarchy> {
  if (_schemaCache && ttl > 0) {
    const age = Date.now() - _schemaCacheTimestamp
    if (age < ttl) return _schemaCache
  }

  const [fkRes, colsRes] = await Promise.all([
    pool.query<{ child_table: string; parent_table: string }>(`
      SELECT
          tc.table_name AS child_table,
          ccu.table_name AS parent_table
      FROM
          information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        AND kcu.column_name = '_parent_id'
        AND LEFT(tc.table_name, 1) != '_'
    `),
    pool.query<{ table_name: string }>(`
      SELECT table_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND column_name = '_locale'
        AND LEFT(table_name, 1) != '_'
    `),
  ])

  const parentToChildren = new Map<string, string[]>()
  for (const { child_table, parent_table } of fkRes.rows) {
    // Skip base collection locales tables (e.g. pages_locales) which Payload handles natively
    if (
      child_table.endsWith('_locales') &&
      !child_table.includes('_blocks_') &&
      !child_table.includes('_array_')
    ) {
      continue
    }

    if (!parentToChildren.has(parent_table)) parentToChildren.set(parent_table, [])
    parentToChildren.get(parent_table)!.push(child_table)
  }

  const tablesWithLocale = new Set(colsRes.rows.map((r) => r.table_name))

  _schemaCache = { parentToChildren, tablesWithLocale }
  _schemaCacheTimestamp = Date.now()
  return _schemaCache
}

// ─── Distributed & Process-Level Mutex (With Safety Timeout) ─────────────────

const _inMemoryLocks = new Map<string, Promise<void>>()

async function acquireDistributedLock(
  pool: pg.Pool,
  lockKey: string,
  timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS,
  failOnError: boolean = false
): Promise<() => Promise<void>> {
  let isReleased = false
  let safetyTimer: NodeJS.Timeout | null = null

  // 1. Process-level mutex
  let releaseInMemory!: () => void
  const newLock = new Promise<void>((resolve) => {
    releaseInMemory = resolve
  })

  const existingLock = _inMemoryLocks.get(lockKey)
  _inMemoryLocks.set(lockKey, newLock)
  if (existingLock) {
    await existingLock
  }

  // 2. PostgreSQL Distributed Advisory Lock
  let advisoryClient: pg.PoolClient | null = null
  try {
    advisoryClient = await pool.connect()
    await advisoryClient.query('SELECT pg_advisory_lock(hashtext($1))', [lockKey])
  } catch (err: any) {
    console.error(`[localeSnapshot] ⚠️ Could not acquire distributed advisory lock for key "${lockKey}":`, err.message)
    if (advisoryClient) {
      advisoryClient.release()
      advisoryClient = null
    }
    if (failOnError) {
      throw err
    }
  }

  const release = async () => {
    if (isReleased) return
    isReleased = true

    if (safetyTimer) {
      clearTimeout(safetyTimer)
      safetyTimer = null
    }

    if (advisoryClient) {
      try {
        await advisoryClient.query('SELECT pg_advisory_unlock(hashtext($1))', [lockKey])
      } catch (e) {
      } finally {
        advisoryClient.release()
        advisoryClient = null
      }
    }

    if (_inMemoryLocks.get(lockKey) === newLock) {
      _inMemoryLocks.delete(lockKey)
    }
    releaseInMemory()
  }

  // Leak-prevention safety timer: if Payload aborts before afterChange runs,
  // ensure the lock and DB connection are always returned to the pool.
  safetyTimer = setTimeout(() => {
    release().catch(() => {})
  }, timeoutMs)

  return release
}

// ─── Snapshot Engine (Universal UUID & Integer Support) ──────────────────────

async function buildSnapshot(
  pool: pg.Pool,
  rootTable: string,
  docId: number | string,
  activeLocale: string,
  debug: boolean,
  maxDepth: number = DEFAULT_MAX_DEPTH,
  schemaCacheTTL: number = DEFAULT_SCHEMA_CACHE_TTL
): Promise<LocaleSnapshot> {
  const { parentToChildren, tablesWithLocale } = await getSchemaHierarchy(
    pool,
    schemaCacheTTL
  )

  const levels: LevelSnapshot[] = []
  let totalRows = 0

  let currentParentTables = [rootTable]
  let currentParentIds: (string | number)[] = [docId]
  let depth = 0

  while (
    currentParentTables.length > 0 &&
    currentParentIds.length > 0 &&
    depth < maxDepth
  ) {
    depth++
    const nextLevelTables: string[] = []
    for (const parent of currentParentTables) {
      const children = parentToChildren.get(parent) || []
      for (const child of children) {
        if (!nextLevelTables.includes(child)) nextLevelTables.push(child)
      }
    }

    if (nextLevelTables.length === 0) break

    const queries: string[] = []
    for (const tbl of nextLevelTables) {
      const safeTblName = tbl.replace(/"/g, '""')
      const safeTblLiteral = tbl.replace(/'/g, "''")
      const hasLocale = tablesWithLocale.has(tbl)

      // Cast "_parent_id"::text to seamlessly support both UUID and Integer/Serial ID schemas
      if (depth === 1) {
        if (hasLocale) {
          queries.push(
            `SELECT '${safeTblLiteral}'::text AS _tbl, row_to_json(t) AS _row FROM "${safeTblName}" t WHERE "_parent_id"::text = $1::text AND "_locale" != $2`
          )
        } else {
          queries.push(
            `SELECT '${safeTblLiteral}'::text AS _tbl, row_to_json(t) AS _row FROM "${safeTblName}" t WHERE "_parent_id"::text = $1::text`
          )
        }
      } else {
        if (hasLocale) {
          queries.push(
            `SELECT '${safeTblLiteral}'::text AS _tbl, row_to_json(t) AS _row FROM "${safeTblName}" t WHERE "_parent_id"::text = ANY($1::text[]) AND "_locale" != $2`
          )
        } else {
          queries.push(
            `SELECT '${safeTblLiteral}'::text AS _tbl, row_to_json(t) AS _row FROM "${safeTblName}" t WHERE "_parent_id"::text = ANY($1::text[])`
          )
        }
      }
    }

    if (queries.length === 0) break

    const unionSql = queries.join('\nUNION ALL\n')
    const params =
      depth === 1
        ? [String(docId), activeLocale]
        : [currentParentIds.map(String), activeLocale]

    const snapRes = await pool.query<{ _tbl: string; _row: RowData }>(unionSql, params)

    const grouped = new Map<string, RowData[]>()
    const nextIds: string[] = []
    const tablesWithRows = new Set<string>()

    for (const { _tbl, _row } of snapRes.rows) {
      if (!grouped.has(_tbl)) grouped.set(_tbl, [])
      grouped.get(_tbl)!.push(_row)
      tablesWithRows.add(_tbl)
      if (_row.id !== undefined && _row.id !== null) {
        nextIds.push(String(_row.id))
      }
    }

    const tableSnapshots: TableSnapshot[] = []
    for (const [tbl, rows] of Array.from(grouped.entries())) {
      tableSnapshots.push({ table: tbl, rows })
      totalRows += rows.length
    }

    if (tableSnapshots.length > 0) {
      levels.push({ level: depth, tables: tableSnapshots })
    }

    currentParentIds = nextIds
    currentParentTables = Array.from(tablesWithRows)
  }

  return { docId, rootTable, levels, totalRows, timestamp: Date.now() }
}

// ─── Durable Fail-Safe Dump ──────────────────────────────────────────────────

function saveDurableSnapshotFallback(snapshot: LocaleSnapshot, reason: string): string {
  try {
    const recoveryDir = path.resolve(process.cwd(), '.locale-snapshot-recovery')
    if (!fs.existsSync(recoveryDir)) {
      fs.mkdirSync(recoveryDir, { recursive: true })
    }
    const filename = `recovery-${snapshot.rootTable}-${snapshot.docId}-${snapshot.timestamp}.json`
    const filePath = path.join(recoveryDir, filename)
    fs.writeFileSync(filePath, JSON.stringify({ reason, snapshot }, null, 2))
    console.warn(`[localeSnapshot] 🛡️ Durable recovery dump saved to: ${filePath}`)
    return filePath
  } catch (err: any) {
    console.error(`[localeSnapshot] Could not save recovery dump:`, err.message)
    return ''
  }
}

// ─── Restore Engine (Atomic Transactional Restoration) ───────────────────────

export async function restoreSnapshot(
  pool: pg.Pool,
  snapshot: LocaleSnapshot
): Promise<{ restoredRows: number; tablesCount: number }> {
  const maxRetries = 3
  let lastError: Error | null = null

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    let client: pg.PoolClient | null = null
    try {
      client = await pool.connect()
      let restoredRows = 0
      let tablesCount = 0

      await client.query('BEGIN')

      // Restore levels sequentially (Level 1 -> Level 2 -> Level 3...) to respect FK hierarchy
      for (const { tables } of snapshot.levels) {
        for (const { table, rows } of tables) {
          if (rows.length === 0) continue

          tablesCount++
          const allColumnsSet = new Set<string>()
          for (const row of rows) {
            for (const key of Object.keys(row)) {
              allColumnsSet.add(key)
            }
          }
          const columns = Array.from(allColumnsSet)
          const colListSql = columns.map((c) => `"${c.replace(/"/g, '""')}"`).join(', ')

          const chunkSize = Math.max(1, Math.floor(CHUNK_PARAM_BUDGET / columns.length))
          for (let i = 0; i < rows.length; i += chunkSize) {
            const chunk = rows.slice(i, i + chunkSize)
            const allValues: unknown[] = []
            const rowPlaceholders: string[] = []
            let paramIdx = 1

            for (const row of chunk) {
              const placeholders: string[] = []
              for (const col of columns) {
                placeholders.push(`$${paramIdx++}`)
                const val = row[col]

                // Handle JSONB and objects (pass Date instances and primitives directly)
                if (val !== null && typeof val === 'object' && !(val instanceof Date)) {
                  allValues.push(JSON.stringify(val))
                } else {
                  allValues.push(val ?? null)
                }
              }
              rowPlaceholders.push(`(${placeholders.join(', ')})`)
            }

            const sql = `INSERT INTO "${table.replace(/"/g, '""')}" (${colListSql}) VALUES ${rowPlaceholders.join(
              ', '
            )} ON CONFLICT ("id") DO NOTHING`

            await client.query(sql, allValues)
            restoredRows += chunk.length
          }
        }
      }

      await client.query('COMMIT')
      return { restoredRows, tablesCount }
    } catch (err: any) {
      lastError = err
      if (client) {
        try {
          await client.query('ROLLBACK')
        } catch (rbErr) {}
      }

      // Exponential backoff on transient lock waits
      if (attempt < maxRetries) {
        await new Promise((r) => setTimeout(r, attempt * 50))
      }
    } finally {
      if (client) client.release()
    }
  }

  // If all retries failed, write durable fallback dump
  saveDurableSnapshotFallback(snapshot, lastError?.message || 'Restore failed after retries')
  throw lastError ?? new Error('Locale restoration failed after retries')
}

/**
 * Replay a saved recovery dump JSON file directly into PostgreSQL.
 */
export async function restoreFromRecoveryDump(
  filePath: string,
  customPool?: pg.Pool
): Promise<{ restoredRows: number; tablesCount: number }> {
  const content = JSON.parse(fs.readFileSync(path.resolve(filePath), 'utf-8'))
  const snapshot: LocaleSnapshot = content.snapshot || content
  const pool = customPool || _fallbackPool || new Pool({ connectionString: process.env.DATABASE_URL })
  return restoreSnapshot(pool, snapshot)
}

// ─── Field Inspector Utility (Deep Recursive) ────────────────────────────────

function hasLocalizedBlocksOrArrays(fields: Field[] = []): boolean {
  for (const field of fields) {
    if ((field.type === 'blocks' || field.type === 'array') && field.localized) {
      return true
    }
    // Recurse into nested container fields (groups, rows, collapsibles)
    if ('fields' in field && Array.isArray((field as any).fields)) {
      if (hasLocalizedBlocksOrArrays((field as any).fields)) return true
    }
    // Recurse into tabs
    if (field.type === 'tabs' && Array.isArray((field as any).tabs)) {
      for (const tab of (field as any).tabs) {
        if (hasLocalizedBlocksOrArrays(tab.fields)) return true
      }
    }
    // Recurse into blocks definitions
    if (field.type === 'blocks' && Array.isArray((field as any).blocks)) {
      for (const block of (field as any).blocks) {
        if (Array.isArray(block.fields) && hasLocalizedBlocksOrArrays(block.fields)) {
          return true
        }
      }
    }
  }
  return false
}

// ─── Plugin Implementation ───────────────────────────────────────────────────

function createPlugin(options: LocaleSnapshotOptions = {}): Plugin {
  return (incomingConfig: Config): Config => {
    const debug = options.debug !== false
    const failOnError = options.failOnError ?? false
    const maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH
    const schemaCacheTTL = options.schemaCacheTTL ?? DEFAULT_SCHEMA_CACHE_TTL
    const lockTimeoutMs = options.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS
    const isServerless = options.serverless ?? false

    // Determine target collections
    const targetCollections = new Set<string>(
      options.collections ??
        (incomingConfig.collections || [])
          .filter((col) => hasLocalizedBlocksOrArrays(col.fields))
          .map((c) => c.slug)
    )

    // Determine target globals
    const targetGlobals = new Set<string>(
      options.globals ??
        (incomingConfig.globals || [])
          .filter((g) => hasLocalizedBlocksOrArrays(g.fields))
          .map((g) => g.slug)
    )

    // Patch Collections
    const patchedCollections: CollectionConfig[] = (incomingConfig.collections || []).map(
      (collection) => {
        if (!targetCollections.has(collection.slug)) return collection

        const rootTable = collection.slug

        return {
          ...collection,
          hooks: {
            ...collection.hooks,

            beforeChange: [
              ...(collection.hooks?.beforeChange ?? []),
              async ({ req, data, originalDoc }: any) => {
                const docId =
                  originalDoc?.id ??
                  data?.id ??
                  req.routeParams?.id ??
                  (req as any)?.id

                if (!docId) return data // New doc creation has no existing translations to guard

                const defaultLocale =
                  (typeof incomingConfig.localization === 'object' &&
                    incomingConfig.localization?.defaultLocale) ||
                  'en'
                const locale = req.locale || defaultLocale

                try {
                  const pool = getDatabasePool(req, incomingConfig)

                  // Acquire Distributed PostgreSQL + in-memory Mutex Lock with leak-proof safety timer
                  const lockKey = `${collection.slug}:${docId}`
                  const releaseLock = await acquireDistributedLock(
                    pool,
                    lockKey,
                    lockTimeoutMs,
                    failOnError
                  )
                  req._localeSnapshotReleaseLock = releaseLock

                  const snapStart = Date.now()
                  const snapshot = await buildSnapshot(
                    pool,
                    rootTable,
                    docId,
                    locale,
                    debug,
                    maxDepth,
                    schemaCacheTTL
                  )
                  const snapMs = Date.now() - snapStart

                  if (snapshot.totalRows > 0) {
                    req._localeSnapshot = snapshot
                    if (debug) {
                      console.log(
                        `[localeSnapshot] 📸 Snapshotted ${snapshot.totalRows} locale rows across ${snapshot.levels.length} levels for ${collection.slug}#${docId} [saving ${locale}] in ${snapMs}ms`
                      )
                    }
                  }
                } catch (err: any) {
                  if (failOnError) {
                    console.error(
                      `[localeSnapshot] ❌ Snapshot failed for ${collection.slug}#${docId} (failOnError=true):`,
                      err.message
                    )
                    throw err
                  }
                  console.error(
                    `[localeSnapshot] ⚠️ Snapshot failed for ${collection.slug}#${docId}:`,
                    err.message
                  )
                }

                return data
              },
            ],

            afterChange: [
              ...(collection.hooks?.afterChange ?? []),
              async ({ req, doc }: any) => {
                const snapshot: LocaleSnapshot | undefined = req._localeSnapshot
                const releaseLock = req._localeSnapshotReleaseLock

                delete req._localeSnapshot
                delete req._localeSnapshotReleaseLock

                if (!snapshot || snapshot.totalRows === 0) {
                  if (releaseLock) await releaseLock()
                  return doc
                }

                const executeRestore = async () => {
                  try {
                    const pool = getDatabasePool(req, incomingConfig)
                    const restoreStart = Date.now()
                    const result = await restoreSnapshot(pool, snapshot)
                    const restoreMs = Date.now() - restoreStart

                    if (debug) {
                      console.log(
                        `[localeSnapshot] ✅ Restored ${result.restoredRows} locale rows across ${result.tablesCount} tables for ${collection.slug}#${doc?.id ?? snapshot.docId} in ${restoreMs}ms`
                      )
                    }
                  } catch (err: any) {
                    console.error(
                      `[localeSnapshot] ⚠️ Restore failed for ${collection.slug}#${doc?.id ?? snapshot.docId}:`,
                      err.message
                    )
                  } finally {
                    if (releaseLock) await releaseLock()
                  }
                }

                if (isServerless) {
                  // Synchronous mode for serverless environments (Vercel, AWS Lambda)
                  await executeRestore()
                } else {
                  // Non-blocking post-commit mode for persistent Node.js / VPS environments
                  setImmediate(executeRestore)
                }

                return doc
              },
            ],
          },
        }
      }
    )

    // Patch Globals
    const patchedGlobals: GlobalConfig[] = (incomingConfig.globals || []).map((global) => {
      if (!targetGlobals.has(global.slug)) return global

      const rootTable = global.slug

      return {
        ...global,
        hooks: {
          ...global.hooks,

          beforeChange: [
            ...(global.hooks?.beforeChange ?? []),
            async ({ req, data, originalDoc }: any) => {
              const docId =
                originalDoc?.id ??
                data?.id ??
                req.routeParams?.id ??
                (req as any)?.id ??
                1

              const defaultLocale =
                (typeof incomingConfig.localization === 'object' &&
                  incomingConfig.localization?.defaultLocale) ||
                'en'
              const locale = req.locale || defaultLocale

              try {
                const pool = getDatabasePool(req, incomingConfig)
                const lockKey = `global:${global.slug}:${docId}`
                const releaseLock = await acquireDistributedLock(
                  pool,
                  lockKey,
                  lockTimeoutMs,
                  failOnError
                )
                req._localeSnapshotReleaseLock = releaseLock

                const snapStart = Date.now()
                const snapshot = await buildSnapshot(
                  pool,
                  rootTable,
                  docId,
                  locale,
                  debug,
                  maxDepth,
                  schemaCacheTTL
                )
                const snapMs = Date.now() - snapStart

                if (snapshot.totalRows > 0) {
                  req._localeSnapshot = snapshot
                  if (debug) {
                    console.log(
                      `[localeSnapshot] 📸 Snapshotted ${snapshot.totalRows} locale rows for global ${global.slug} [saving ${locale}] in ${snapMs}ms`
                    )
                  }
                }
              } catch (err: any) {
                if (failOnError) {
                  console.error(
                    `[localeSnapshot] ❌ Snapshot failed for global ${global.slug} (failOnError=true):`,
                    err.message
                  )
                  throw err
                }
                console.error(
                  `[localeSnapshot] ⚠️ Snapshot failed for global ${global.slug}:`,
                  err.message
                )
              }

              return data
            },
          ],

          afterChange: [
            ...(global.hooks?.afterChange ?? []),
            async ({ req, doc }: any) => {
              const snapshot: LocaleSnapshot | undefined = req._localeSnapshot
              const releaseLock = req._localeSnapshotReleaseLock

              delete req._localeSnapshot
              delete req._localeSnapshotReleaseLock

              if (!snapshot || snapshot.totalRows === 0) {
                if (releaseLock) await releaseLock()
                return doc
              }

              const executeRestore = async () => {
                try {
                  const pool = getDatabasePool(req, incomingConfig)
                  const restoreStart = Date.now()
                  const result = await restoreSnapshot(pool, snapshot)
                  const restoreMs = Date.now() - restoreStart

                  if (debug) {
                    console.log(
                      `[localeSnapshot] ✅ Restored ${result.restoredRows} locale rows for global ${global.slug} in ${restoreMs}ms`
                    )
                  }
                } catch (err: any) {
                  console.error(
                    `[localeSnapshot] ⚠️ Restore failed for global ${global.slug}:`,
                    err.message
                  )
                } finally {
                  if (releaseLock) await releaseLock()
                }
              }

              if (isServerless) {
                await executeRestore()
              } else {
                setImmediate(executeRestore)
              }

              return doc
            },
          ],
        },
      }
    })

    return {
      ...incomingConfig,
      collections: patchedCollections,
      globals: patchedGlobals,
    }
  }
}

/**
 * Universal Plugin Export:
 * Supports both `plugins: [localeSnapshot]` and `plugins: [localeSnapshot({ ... })]`
 */
export function localeSnapshot(optionsOrConfig?: LocaleSnapshotOptions | Config): any {
  if (optionsOrConfig && isConfig(optionsOrConfig)) {
    // Invoked directly with Payload config: `plugins: [localeSnapshot]`
    return createPlugin({})(optionsOrConfig as Config)
  }
  // Invoked as a factory: `plugins: [localeSnapshot({ ... })]`
  return createPlugin(optionsOrConfig as LocaleSnapshotOptions)
}

/** Backward-compatible alias */
export const localeGuard = localeSnapshot

/**
 * Type guard to distinguish full Payload Config from LocaleSnapshotOptions
 */
function isConfig(obj: any): boolean {
  if (!obj || typeof obj !== 'object') return false

  const configOnlyKeys = ['db', 'localization', 'serverURL', 'routes', 'admin', 'typescript']
  for (const key of configOnlyKeys) {
    if (key in obj) return true
  }

  if ('collections' in obj && Array.isArray(obj.collections)) {
    const first = obj.collections[0]
    if (first && typeof first === 'object' && 'slug' in first) {
      return true
    }
  }

  return false
}

export default localeSnapshot
