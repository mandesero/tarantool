# Native VECTOR implementation contract

RFC revision 3 is normative. This document covers the native index.
The distributed API contract is maintained in the companion
[CRUD guide](https://github.com/mandesero/crud/blob/codex/vector-search/doc/vector-search.md).
Build and verification evidence is in [report.md](report.md).

## Numeric and search behavior

- The production algorithm is memtx HNSW with a replaceable internal backend.
- Dense vectors use f32 and distances use f64 (`f32_f64_v1`). Tuple arrays
  remain unchanged. Inputs are dense finite arrays of the exact dimension.
- `l2` is squared Euclidean distance; `cosine` is one minus normalized
  dot product; `ip` is one minus dot product. Smaller distances rank first.
- Local search uses `index:select({query}, {iterator = 'neighbor', limit = k,
  with_distance = true, timeout = seconds, filter = filter, opts = opts})`.
- The result distance belongs to the returned visible tuple version.
  Equal distances use primary-key order with `key_def` semantics.
- The query-time HNSW option is `ef_search`; its effective value is
  `max(request_or_index_ef_search, limit)`.
- A filter is an unsigned field and a finite set of unsigned values.
  Filtering occurs before limiting candidates.
- ANN may normally return fewer than the requested limit. Deadline and
  work exhaustion are errors without partial success.
- A zero limit validates input and options and returns an empty array.
  Positive-limit empty results still register an index-wide MVCC dependency.
- Linearizable VECTOR reads and public `box.read_view` searches are rejected.

## Ownership, transactions and DDL

Labels are monotonically assigned uint64 values per index generation.
Each label resolves to a version record with a tuple reference, dense vector
and lifecycle state. A label cannot be recycled until graph and MVCC GC
agree. Navigation tombstones may retain vectors after tuple release.

The per-index `ann_memory` context charges graph capacity and retained
versions to memtx quota. Temporary searches use a bounded operation context.
DML failures restore the previous index state without allocating during undo.

Changing dimension, distance, algorithm, `m` or `ef_construction` builds a
new generation and swaps only after success. Changing default `ef_search`
updates the search configuration. Online rebuild requires quota for both
generations. A maintenance drop and recreate handles insufficient headroom.

## Server bounds

| Parameter | Bound or default |
| --- | --- |
| dimension | 1 to 4096 |
| local limit | 0 to 1024; explicit |
| HNSW m | 4 to 64; default 16 |
| ef_construction | m to 8192; default 200 |
| ef_search | 1 to 8192; default 64 |
| filter values | at most 65536 |
| timeout | positive finite, at most 30 s; default 1 s |
| operation work | at most 2^32 component operations |
| temporary search memory | at most 64 MiB |

These are server policy choices. Distributed participant and reply bounds
are enforced by CRUD and documented there.

## Index operations

| Operation | Behavior |
| --- | --- |
| insert, replace, update, upsert, delete | supported with undo |
| `len`, `bsize` | supported and separately defined |
| `select` NEIGHBOR | supported only with explicit limit |
| `get`, `min`, `max`, `random` | reject early |
| `pairs`, keyed `count` | reject early |
| unkeyed `count` | return `len` |
| offset, position, continuation | reject early |
| functional, multikey or nullable vector part | reject at DDL |
| public `box.read_view` search | reject early |

No iterator silently ignores VECTOR-specific options.

## Diagnostics and introspection

The kernel reports `ER_VECTOR_INVALID`, `ER_VECTOR_UNSUPPORTED`,
`ER_VECTOR_TIMEOUT` and `ER_VECTOR_WORK_LIMIT`. Allocation failure uses the
standard OutOfMemory diagnostic.

`index:stat()` returns `{config, limits, versions, slots, memory, search, hnsw}`.
Configuration names dimension, distance, algorithm, scalar representation,
numeric contract and effective defaults. Limits name maximum dimension,
result count, search width, filter values, timeout and work, plus the default
timeout. They describe production limits, excluding debug error injections.
Versions distinguish live, retained and retired counts. Slots report capacity
and `pending_rebuild` counts. Retired navigation nodes and their dense vectors
remain until rebuild; insertions do not reuse these slots. Ordinary memtx
releases obsolete tuple bindings after commit or rollback; MVCC does so in
story GC after dependent readers finish. Memory includes graph, vectors,
lookup, retained,
temporary peak and totals by generation. Search counters are cumulative
requests, errors, timeouts, visited and filtered candidates. `duration_ns`
counts time in admitted C searches, including query decoding, sorting, port
materialization, cleanup, failures and zero-limit calls. It excludes Lua-side
validation and conversion after the C call. `hnsw` contains
algorithm-specific counters. `bsize` excludes tuple and per-request storage.

The private `box.internal.vector_icu_version()` helper exposes the runtime
version used by native collations. CRUD combines it with primary-key and
collation fingerprints before merging distributed results. ICU remains a
native dependency; no router/storage procedure is embedded in the binary.

## Acceptance map

| Decision | Check |
| --- | --- |
| Numeric formulas and f32 range | Independent scalar oracle |
| Backend ownership, undo and stop | Flat/USearch unit and fail-nth |
| DML and MVCC lifetime | Failure, WAL and read-view tests |
| Limits, defaults and unsupported operations | Rejection Luatest |
| DDL and rebuild | Atomic rebuild and recovery |
| `stat()` and `bsize` | Memory and counter checks |
| Sync writes and replication | Native transaction and replica suites |
| Recall and resource use | Seeded native benchmark matrix |
