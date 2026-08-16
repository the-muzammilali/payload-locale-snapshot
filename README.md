# payload-locale-snapshot

[![npm version](https://img.shields.io/npm/v/payload-locale-snapshot.svg)](https://www.npmjs.com/package/payload-locale-snapshot)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A zero-loss database stability plugin for **Payload CMS 3.x** using the **PostgreSQL adapter (`@payloadcms/db-postgres`)**. It guarantees zero data loss of localized blocks, nested arrays, and sub-items across all languages when saving documents in any locale.

---

## The Problem

When using Payload CMS 3.x with PostgreSQL, collections with `localized: true` block or array fields are vulnerable to silent data loss during routine editorial changes in the admin UI or API.

When saving an update for a document (e.g. in the default English locale), Payload's internal Drizzle adapter executes a cascade delete:
```sql
DELETE FROM "pages_blocks_*" WHERE "_parent_id" = <documentId>;
```

Because this delete is not scoped by `_locale`, PostgreSQL cascades and deletes **every translated block and nested sub-item across all other languages**. Payload then only re-inserts the blocks for the currently active locale, wiping out all other translations.

---

## The Solution

`payload-locale-snapshot` hooks directly into Payload's `beforeChange` and `afterChange` lifecycles:

1. **`beforeChange` Hook**:
   - Queries PostgreSQL using a multi-table, parameterized query that snapshots all block, array, and child items belonging to non-active locales directly into `req._localeSnapshot`.
   - Traverses the database schema in topological hierarchy order (Level 1 $\to$ Level 2 $\to$ Level 3...).
   - Supports integer, serial, text, and UUID (`idType: 'uuid'`) primary keys with universal `::text` casting.
   - Acquires both in-memory and **PostgreSQL Distributed Advisory Locks (`pg_advisory_lock`)** with a safety auto-release timeout to prevent connection leaks.

2. **`afterChange` Hook**:
   - **Persistent Node.js / VPS (Default)**: Dispatches restoration via `setImmediate`, allowing Payload's active database transaction to commit and release parent row and foreign key locks immediately.
   - **Serverless Environments (`serverless: true`)**: Direct synchronous `await` inside the hook lifecycle for short-lived runtimes (AWS Lambda, Vercel).
   - Atomically restores snapshotted rows in strict topological order inside a dedicated `BEGIN ... COMMIT / ROLLBACK` PostgreSQL transaction with `ON CONFLICT ("id") DO NOTHING`.
   - Includes an **automatic lock-contention retry loop** with exponential backoff.
   - Saves a **durable JSON recovery dump** (`.locale-snapshot-recovery/`) in the event of an unexpected database connection drop.
   - Releases all locks safely in a `finally` block once restoration completes.

---

## Key Features & Guarantees

- 🚀 **Zero Translation API Costs**: Directly preserves and restores database rows with zero external API calls.
- 🔑 **Universal ID Compatibility**: Seamlessly supports Integer, Serial, and UUID (`idType: 'uuid'`) primary key schemas.
- 🔒 **Distributed Concurrency Safe**: Uses native PostgreSQL session advisory locking (`pg_advisory_lock`) with fail-safe release timers.
- ⚡ **Serverless & VPS Ready**: Supports non-blocking post-commit dispatch on VPS as well as synchronous awaiting on serverless runtimes via `serverless: true`.
- 🔍 **Dynamic Schema Discovery**: Automatically discovers all block, array, and sub-item tables directly via PostgreSQL foreign key constraints with TTL-based caching.
- 🌐 **Deep Recursive Auto-Detection**: Deep recursive field inspection without hardcoded collection slugs.
- 🛡️ **True `_locale` Filtering**: Filters by `"_locale" != activeLocale`, protecting both script-translated content and blocks created or modified manually in the Payload Admin UI.
- 📐 **Topological FK Ordering**: Restores parent tables before child tables, ensuring foreign key constraints are never violated.
- 🔄 **Lock-Contention Auto-Retry**: Automatically retries with exponential backoff on transient lock waits.
- 💾 **Durable Fail-Safe Dump**: Automatically preserves snapshot to disk if the database connection drops during restore.
- 🛠️ **Offline Replay Utility**: Exported `restoreFromRecoveryDump()` helper to re-apply any recovery dump in one line.

---

## Installation

```bash
npm install payload-locale-snapshot
# or
pnpm add payload-locale-snapshot
# or
yarn add payload-locale-snapshot
```

### Peer Dependencies
Make sure you have `payload` (3.x) and `@payloadcms/db-postgres` installed in your project.

---

## Usage

### 1. Register in `payload.config.ts`

Import and add `localeSnapshot` to your `plugins` array:

```typescript
import { buildConfig } from 'payload'
import { localeSnapshot } from 'payload-locale-snapshot'

export default buildConfig({
  // ...other configuration
  plugins: [
    localeSnapshot,
    // Or with custom options:
    // localeSnapshot({
    //   collections: ['pages', 'products', 'solutions'],
    //   globals: ['settings', 'header'],
    //   debug: true,
    //   failOnError: false,
    //   maxDepth: 10,
    //   schemaCacheTTL: 300_000,
    //   lockTimeoutMs: 30_000,
    //   serverless: false,
    // }),
  ],
})
```

---

## Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `collections` | `string[]` | Auto-detected | Specific collection slugs to guard. If omitted, collections with localized blocks or arrays are automatically guarded. |
| `globals` | `string[]` | Auto-detected | Specific global slugs to guard. If omitted, globals with localized blocks or arrays are automatically guarded. |
| `debug` | `boolean` | `true` | Enables detailed logging of snapshot and restore performance metrics in the server console. |
| `failOnError` | `boolean` | `false` | When `true`, a snapshot failure in `beforeChange` throws an error and aborts the save. |
| `maxDepth` | `number` | `10` | Maximum depth for foreign key hierarchy traversal. |
| `schemaCacheTTL` | `number` | `300000` (5 min) | Time-to-live for the schema cache in milliseconds. Set to `0` to disable caching. |
| `lockTimeoutMs` | `number` | `30000` (30s) | Safety timeout for advisory locks. Auto-releases locks and connections if a save aborts. |
| `serverless` | `boolean` | `false` | When `true`, awaits restore synchronously in `afterChange` for short-lived serverless runtimes. |

---

## Utility Exports

### `resetSchemaCache()`
Manually invalidates the schema hierarchy cache. Useful when database migrations add new block types at runtime:

```typescript
import { resetSchemaCache } from 'payload-locale-snapshot'

// Invalidate cache after running migrations
resetSchemaCache()
```

### `restoreFromRecoveryDump(filePath, pool?)`
Replays an offline snapshot dump directly back into PostgreSQL:

```typescript
import { restoreFromRecoveryDump } from 'payload-locale-snapshot'

await restoreFromRecoveryDump('.locale-snapshot-recovery/recovery-pages-1-1786884000000.json')
```

---

## Requirements

- Payload CMS `^3.0.0`
- PostgreSQL 14+ with `@payloadcms/db-postgres`
- Node.js `>=18.20.0`

---

## License

[MIT](LICENSE) © [Muzammil Ali](https://github.com/the-muzammilali)
