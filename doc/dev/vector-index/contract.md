# Native VECTOR implementation contract

The RFC revision dated 2026-11-09 is normative. This document covers the
native index.
The distributed API contract is maintained in the companion
[CRUD guide](https://github.com/mandesero/crud/blob/codex/vector-search/doc/vector-search.md).
Build and verification evidence is in [report.md](report.md).

## Numeric and search behavior

- The production algorithm is memtx HNSW with a replaceable internal backend.
- Dense vectors use f32 and distances use f64 (`f32_f64_v1`). Tuple arrays
  remain unchanged. Inputs are dense finite arrays of the exact dimension.
- `l2` is squared Euclidean distance; `cosine` is one minus normalized
  dot product; `ip` is one minus dot product. Smaller distances rank first.
- Ordinary local and IProto SELECT use
  `index:select({query}, {iterator = 'neighbor', limit = k})` and return tuples.
  Both local FFI and Lua C enter the shared native search statement.
- Rich local search uses `index:search(query, {limit = k, with_distance = true,
  timeout = seconds, filter = filter, opts = opts})`. It returns two arrays,
  `tuples, distances`, or only `tuples` without `with_distance`.
- Rich search and rebuild enter Lua C, including from JIT-compiled callers.
  Ordinary SELECT rejects ANN options, offset and cursor options. `net.box`
  keeps its standard transport options; its timeout does not set ANN work.
- The result distance belongs to the returned visible tuple version.
  Equal distances use primary-key order with `key_def` semantics.
- The query-time HNSW option is `ef_search`; its effective value is
  `max(request_or_index_ef_search, limit)`.
- A filter is an unsigned field and a finite set of unsigned values.
  Filtering occurs before limiting candidates.
- ANN may normally return fewer than the requested limit. Deadline and
  work exhaustion are errors without partial success.
- A zero limit validates input and options and returns an empty tuple array
  (and an empty distance array with `with_distance`).
  Positive-limit empty results still register an index-wide MVCC dependency.
- Linearizable VECTOR reads and public `box.read_view` searches are rejected.

## Ownership, transactions and DDL

Labels identify immutable embeddings, not individual tuple versions.
Several versions of the same primary key share one graph node when their
canonical f32 vectors match. A payload-only update keeps the vector and graph
node; visibility and filters still select the exact tuple version. GC recycles
binding and embedding labels. Graph slots keep navigation coordinates until
compaction. Generations share reference-counted coordinates, charged once.

Each generation has a separate `ann_memory` owner. Shared coordinates and
version bindings have independent owners. Allocations use malloc charged to
the shared memtx quota, not the slab arena. DML failures restore the previous
state without allocating during undo, including failure in a later secondary
index or either graph during rebuild. Visibility-list traversal consumes the
same query work and time budgets as graph traversal.

Changing dimension, distance, algorithm, `m` or `ef_construction` builds a new
generation and swaps only after success. Changing default `ef_search` updates
the search configuration. HASH primary keys cannot support normal yielding
build on a nonempty space; that operation remains rejected.

Rebuild pins the index and visits a fixed initial range of embedding labels.
Concurrent new embeddings enter both graphs atomically; payload updates and
version GC use their shared binding registry. DML and GC do not restart replay.
Only index replacement or DROP invalidates its weak reference. Publication
has no yield. Before publication, cancellation or allocation failure discards
the private graph; afterward old-graph cleanup finishes even if cancelled.
Manual `rebuild()` waits for completion. Simultaneous rebuilds and invocation
inside the calling fiber's transaction are rejected. The builder and cleanup
yield between portions with a 1 ms target; a single graph operation can take
longer, so this is not a strict TX latency bound.

A background fiber compacts when at least 64 navigation-only nodes occupy
at least one quarter of the current graph. The ordinary write path maintains
unused quota headroom of four times the current graph owner's charged bytes
plus 64 KiB. Rebuild transfers this reservation to its private owner, and
recycles temporary maintenance charges within it. Concurrent writes cannot
consume it. New embeddings admitted during one rebuild are capped at
`max(64, initial_embedding_count / 4)`; exceeding the cap returns OutOfMemory
without changing the tuple or aborting rebuild. DML uses unreserved quota.
This bounds concurrent growth while the finite initial replay progresses.
The policy constants are internal safeguards, not measured optimal values.

Successful completion requires sufficient reserved memory, allocator success
and scheduling of the builder. Long transactions can legitimately retain old
versions; they are not cancelled by maintenance. Quota exhaustion rejects
new allocations rather than silently discarding needed versions. Data growth
is still subject to memtx quota. Garbage and temporary growth are bounded by
automatic compaction, reservation and admission limits.

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
| `select` NEIGHBOR | local and native IProto; explicit limit |
| `search` | rich local API; flat query; optional parallel distances |
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

`index:stat()` returns `{config, limits, versions, slots, memory, search,
maintenance, hnsw}`.
Configuration names dimension, distance, algorithm, scalar representation,
numeric contract and effective defaults. Limits name maximum dimension,
result count, search width, filter values, timeout and work, plus the default
timeout. They describe production limits, excluding debug error injections.
`versions.live` is the current primary-index size; `versions.retained` is
`max(retained_tuple_bindings - live, 0)`. `versions.retired` counts inactive
graph nodes. `slots.embeddings` counts embeddings with tuple bindings.
Slots also report graph capacity and `pending_rebuild` navigation-only nodes.
Ordinary memtx releases obsolete bindings after commit or rollback; MVCC does
so in story GC after dependent readers finish. Bindings and storage labels
are reused independently of graph slots.

Memory includes graph, vectors, lookup, retained, temporary peak and totals.
`vectors` counts distinct coordinates with tuple bindings; `retained` counts
coordinates kept only by graph navigation. `generations.current` includes the
active graph and shared owners; `generations.transient` counts the private
graph being built or reclaimed. Coordinates are never double counted.
`memory.total` and `bsize()` include both generations, excluding tuple storage,
region memory, quota rounding and system allocator overhead; they are not RSS.
`memory.maintenance_reserved` is unused quota credit, not allocated bytes, and
is reported separately from total and bsize. `maintenance` exposes completed
and failed rebuild counts, running state, admitted insertions and their cap.
Search counters are cumulative
requests, errors, timeouts, visited and filtered candidates. `duration_ns`
counts time in admitted C searches, including query decoding, sorting, port
materialization, cleanup, failures and zero-limit calls. It excludes Lua-side
validation and conversion into Lua result tables in the binding. `hnsw` contains
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
