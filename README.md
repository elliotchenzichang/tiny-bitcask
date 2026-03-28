# tiny-bitcask

[tiny-bitcask](https://github.com/elliotchenzichang/tiny-bitcask) is a small [Bitcask](https://riak.com/assets/bitcask-intro.pdf)-style key/value store in Go: one active append-only data file per directory, an in-memory **keydir** (hash map) pointing at the latest record per key, and optional **merge** to drop stale records and reclaim space.

This repo is an educational reference and a playground for experiments; `master` carries ongoing work. For a smaller, teaching-oriented snapshot, use the **`demo`** branch:

```shell
git clone git@github.com:elliotchenzichang/tiny-bitcask.git
cd tiny-bitcask
git checkout demo
```

---

## Design (aligned with the paper)

| Idea | Role in this project |
|------|----------------------|
| Append-only active file | Writes go to `ActiveFile`; when size exceeds `SegmentSize`, the file is sealed and a new active file is opened (`storage/datafiles.go`). |
| Hint files | After rotation, each sealed `fid.dat` can have a compact `fid.hint` for faster recovery; invalid or missing hints fall back to scanning the data file (`storage/hint.go`, `db.go` recovery). |
| Keydir | `index.KeyDir` maps string key → `DataPosition` (file id, offset, key/value sizes, timestamp). |
| Read path | One hash lookup + one `ReadAt` by `(fid, offset, length)`; optional **CRC32** verification on read (`Options.VerifyCRC`, default `true`). |
| Merge / compaction | Scans **immutable** files and rewrites entries that are still the live version into the active file, then deletes merged files (`DB.Merge`). Live vs. stale is decided by comparing the keydir’s `(fid, offset)` to the **start** offset of each record while scanning. |
| Tombstone delete | Deletes append a record with `DeleteFlag`; the key is written in the record for recovery; the key is removed from the keydir (`DB.Delete`). |

---

## What is implemented

- **Open / create**: `NewDB` — empty directory creates a new store; existing directory **recovers** the keydir by scanning `*.dat` files in order (or hints for sealed segments). **`Options`**: `VerifyCRC` (default on), `ReadOnly` (open existing store read-only), `ExclusiveLock` (Unix advisory `flock` on `.tiny-bitcask.lock`; shared lock when `ReadOnly`).
- **Hint files**: On **segment rotation**, a compact **`fid.hint`** is written next to the sealed **`fid.dat`** (atomic write). Hint entries omit values; tombstone records are skipped. When **merge** removes an old segment, the matching **`.hint`** is removed with it.
- **Put / Get / Delete**: basic APIs with a process-wide `RWMutex`.
- **ListKeys / Fold**: `ListKeys` returns keys sorted lexicographically; `Fold` walks keys in that order and reads each value (read lock held for the scan).
- **Sync / Close**: `DB.Sync` fsyncs the active segment; `DB.Close` syncs then closes segment files and releases the lock file handle.
- **Segment rotation**: configurable `Options.SegmentSize` (default 256 MiB).
- **On-disk record layout**: fixed meta (CRC32, timestamps, sizes, flag) + key + value (`entity/entry.go`). Tombstone records store the key with `ValueSize` 0.
- **Merge**: rewrites live entries from old segments and removes merged files; tombstone records in old files are skipped during merge.
- **CRC on read**: Enabled by default; disable with `Options.VerifyCRC = false` if needed.
- **Recovery**: Full segment scans apply tombstones in order (remove key from keydir) and populate `DataPosition.Timestamp` from record meta; hint recovery skips tombstone rows.
- **Tests**: `db_test.go` covers CRUD, rotation, merge, delete+merge, hint recovery, merge after reopen, tombstone recovery, CRC failure, ListKeys/Fold, read-only open; `storage/hint_test.go` and `entity/entry_test.go` cover hint encoding and CRC/tombstones (requires `github.com/stretchr/testify`).

---

## Gaps vs. the Bitcask paper and production-grade behavior

Some items from [bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf) and typical production engines are still out of scope or partial:

1. **Merge + hint generation** — Merge rewrites live data into the active file and deletes merged segments (and their hints); a new hint is written only when that segment is **rotated** (normal append-only behavior).
2. **Portability** — Advisory locking is implemented on Unix (`flock`). On other platforms the lock is a no-op; use a single process or external coordination.
3. **API breadth** — No key prefix / range iterator beyond sorted `ListKeys` + `Fold`. No snapshot or MVCC reads.
4. **Durability policy** — `Sync` is explicit; there is no `Sync` after every write (call `Sync` when you need durability beyond process crash).

---

## Code map

| Path | Purpose |
|------|---------|
| `db.go` | `NewDB`, `Get` / `Set` / `Delete`, `Merge`, `ListKeys`, `Fold`, `Sync`, `Close`, `recovery` |
| `lock_unix.go`, `lock_other.go` | Optional advisory DB lock |
| `index/index.go` | Keydir (`map` + `DataPosition`) |
| `storage/datafiles.go` | Active/old files, rotation, read/write entries, CRC, `Sync`/`Close` |
| `storage/hint.go` | Hint file format, write on rotation, read/remove with segments |
| `entity/entry.go` | Binary encoding of records, tombstones, `VerifyRecordCRC` |
| `options.go` | `Dir`, `SegmentSize`, `VerifyCRC`, `ReadOnly`, `ExclusiveLock` |

---

## Todo (paper-aligned and robustness)

- [x] **Hint files**: format, write on rotation, load hint instead of full scan on open when present.
- [x] **Recovery**: populate old-file ID list from on-disk segments so `Merge` works after restart; timestamps from full scan recovery.
- [x] **Merge**: compare keydir `(fid, offset)` to the **start** offset of each scanned entry; test `TestDB_Merge_LiveKeyOnlyInOldSegment`.
- [x] **Delete / tombstones**: valid meta + key for tombstone records; recovery applies tombstones; merge skips tombstone bodies.
- [x] **CRC verification** on read (`Options.VerifyCRC`, default `true`).
- [x] **`Sync` / `Close`**: flush and fsync active file; close FDs and lock.
- [x] **API**: `ListKeys`, `Fold`, read-only open (`Options.ReadOnly`), Unix advisory lock (`Options.ExclusiveLock`).
