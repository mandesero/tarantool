# Vector index implementation contract

RFC revision 3 is normative. The policy choices below are exercised by
the implementation tests. The build and verification evidence is in
[report.md](report.md).

## Stable RFC behavior

- Production algorithm: HNSW on memtx, with a replaceable internal backend.
- Dense storage: float32; distance arithmetic and result: float64, contract
  `f32_f64_v1`. Tuple array values remain unchanged.
- Distance names and formulas: `l2` is squared Euclidean distance,
  `cosine` is one minus normalized dot product, and `ip` is one minus dot.
- Local search: `index:select({query}, {iterator = 'neighbor', limit = k,
  with_distance = true, timeout = seconds, filter = filter, opts = opts})`.
  `limit` is explicit; tuple-only and distance-bearing modes share a search.
- Every result has the distance of its returned, visible tuple version.
  Equal distances are ordered by primary key using `key_def` semantics.
- The only query-time HNSW option is `ef_search`; the effective value is
  `max(request_or_index_ef_search, limit)`.
- A filter is one unsigned field and a finite set of unsigned values.
  Filtering happens before limiting returned candidates.
- ANN may return fewer than `limit` on normal completion. Timeout and work
  exhaustion are errors without a partial result.
- In MVCC, an ANN read registers a dependency on the whole VECTOR index,
  including an empty positive-limit result. Linearizable VECTOR is rejected.
- The wire identity is `(bucket_id, id)`, where `id` is an array of PK parts.
  The wire sort is distance, numeric bucket_id, then PK by `key_def` rules.
- Storage returns an envelope with protocol version, dimension, distance,
  scalar, numeric contract, PK comparator metadata, covered bucket IDs,
  and an array of records.
  Each record is `{id, bucket_id, vector, distance}`. Empty output is `[]`.
- One bucket uses bucket-aware `callrw`; global and bucket-set scopes use
  full `map_callrw` on masters. Every required response must succeed.
- Router returns the first k of a merge of received candidates. It does
  not promise exact global top-k or a cluster-wide MVCC snapshot.

## Implementation choices

- Use monotonically assigned uint64 labels per index generation. A label
  resolves to a version record containing a tuple reference, dense vector,
  and lifecycle state. Do not recycle a label until graph and MVCC GC agree.
- Keep tuple and dense-vector lifetimes separate: a navigation tombstone may
  retain the dense vector after its tuple reference can be released.
- Own all backend memory through a per-index `ann_memory` context. Charge
  capacity and old generations to memtx quota; charge temporary search
  storage to a bounded operation context.
- Changing dimension, distance, algorithm, `m`, or `ef_construction`
  rebuilds a new generation and atomically switches after success.
  Changing default `ef_search` changes only search configuration.
- An online rebuild needs headroom for both generations. A maintenance
  drop-and-create path handles insufficient headroom.
- Rebuild a router-side `key_def` with wire PK parts renumbered to key
  array positions. Use `key_def:compare_keys` on exact MsgPack values;
  never convert uint64 cdata to Lua number. Include collation rule
  fingerprints and runtime version in metadata and reject mismatches.
- Use protocol envelope version 1. Reject unknown versions and incompatible
  dimension, distance, scalar, numeric contract, PK part types, order, or
  collation identity before merge.
- The package name is `vector_search`, with `storage` and `router` modules.
  Its public entry points and defaults are specified below.

## Server bounds and defaults

| Parameter | Bound or default |
| --- | --- |
| dimension | 1 to 4096 |
| local limit, k, L | 0 to 1024; limit and k explicit |
| HNSW m | 4 to 64; default 16 |
| ef_construction | m to 8192; default 200 |
| ef_search | 1 to 8192; default 64 |
| filter values | at most 65536 |
| local timeout | positive finite, at most 30 s; default 1 s |
| router timeout | positive finite, at most 30 s; default 1 s |
| operation work | at most 2^32 component operations |
| temporary search memory | at most 64 MiB |
| total serialized response | at most 32 MiB |

Bounds above are server policy choices, not values from the RFC.
The max-result size must be checked against dimension and participant count
before dispatch, then against actual MsgPack output after each response.
The DDL, select, storage, and router Luatest suites check these bounds.

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
| functional or multikey part | reject at DDL |
| nullable vector part | reject at DDL |
| public `box.read_view` search | reject early |

A zero limit still validates the query and options, then returns an empty
array. A positive limit on an empty or filtered-out index still registers
an index-wide MVCC read dependency. No iterator API may silently ignore
VECTOR-specific options.

## Lua package API

The router entry point is `vector_search.router.search(name, query, opts)`.
Configure it with `vector_search.router.configure(name, {dimension,
distance, pk_parts})` before searching. `pk_parts` is an array of primary
key part definitions with `type`, optional `sort_order`, and optional
`collation`. Configuration may also set `max_participants` (128),
`max_pk_bytes` (1024), and `max_response_bytes` (32 MiB). Only admin may
configure the router and storage modules.
`name` is a configured logical index name. `opts` requires `k` and may
contain `L`, `scope`, `timeout`, and `algorithm_opts`. Default L is k,
default scope is all, default timeout is 1 s, and default algorithm
options are empty. `scope` is exactly one
of `{kind = 'all'}`, `{kind = 'bucket', bucket_id = id}`, or
`{kind = 'buckets', bucket_ids = {...}}`. Empty `bucket_ids` is empty
scope. Unknown or conflicting fields are errors.

The storage procedure is
`vector_search.storage.search(request, remaining_timeout)` and is
registered for IProto CALL only after explicit configuration and grants.
`request` carries the logical name, query, L, scope, and algorithm options.
The second argument carries a relative budget injected after the vshard
ref phase, or computed before bucket-aware `callrw`. The procedure
returns one versioned envelope or raises a structured error; it never
uses `return nil, err` as its sole error transport through `storage_map`.
`covered_bucket_ids` is an array of bucket IDs protected by the call and
included in the local filter. The router checks complete, disjoint
coverage before returning records.

## Wire PK comparison

The wire metadata carries each PK part's type, order, field position,
nullable flag, decimal scale, collation name and ID, full collation rules
fingerprint, and ICU runtime version when applicable. The router reconstructs a
`key_def` over the transmitted key arrays and calls `compare_keys`.
A missing or incompatible comparator rejects the whole request. The
ordered scalar types accepted by `key_def` and MsgPack are allowed;
functional, multikey, or nullable PK definitions are rejected for this
version. A `float32` PK is rejected during configuration: MsgPack decode
widens its values to Lua `number`, and `key_def` cannot compare that
value with a `float32` part. The exact scalar-type matrix is exercised
in `test/box-luatest/vector_router_test.lua`.

## Diagnostics and introspection

The kernel uses `ER_VECTOR_INVALID` for invalid query, vector, filter,
limit, or algorithm options; `ER_VECTOR_UNSUPPORTED` for a rejected
VECTOR operation or isolation mode; `ER_VECTOR_TIMEOUT` for deadline
exhaustion; and `ER_VECTOR_WORK_LIMIT` for work-budget exhaustion.
Allocation failure uses Tarantool's standard OutOfMemory diagnostic.
The Lua package uses `VECTOR_PROTOCOL`, `VECTOR_COVERAGE`, and
`VECTOR_REMOTE` structured codes for incompatible, incomplete, or
failed distributed replies. Error codes must be added and tested with
the behavior they report, not introduced as dormant placeholders.

`index:stat()` returns `{config, versions, slots, memory, search, hnsw}`.
`config` names dimension, distance, algorithm, and effective defaults.
`versions` has live, retained, and retired counts. `slots` has capacity
and reusable counts. `memory` has graph, vectors, lookup, retained,
temporary peak, and total bytes, plus per-generation totals. `search`
has cumulative requests, errors, timeouts, visited candidates, and
filtered candidates. `hnsw` contains algorithm-specific counters.
Each field must document whether it is current, cumulative, or an
estimate. `bsize` includes index-owned resident memory, excluding tuple
storage and per-request temporary memory.

## Placement and acceptance map

The `vector_search` package lives in `src/box/lua/vector_search/` in this
repository, with `storage.lua` and `router.lua` as separate modules. It
loads vshard only when configured. The core build registers both modules
through `src/box/CMakeLists.txt` and the box Lua module table. Local
storage tests are under `test/box-luatest/`. The router/storage cluster
fixture is under `test/vshard-vector/` and runs against the pinned
companion vshard fork. These paths are implementation choices, not
normative RFC paths.

| Decision | Required check |
| --- | --- |
| Numeric formulas and f32 range | Independent scalar oracle |
| Backend ownership and stop | Flat/USearch unit and fail-nth |
| DML, undo, MVCC lifetime | DML failure, WAL, read-view tests |
| All server limits and defaults | Boundary and rejection Luatest |
| DDL alter and rebuild | Failure-atomic rebuild/recovery |
| Unsupported index operations | Early diagnostic Luatest |
| `index:stat()` and `bsize` | Memory and counter checks |
| Wire version and metadata | Schema mismatch and merge tests |
| PK types and collation identity | Exact MsgPack/key_def matrix |
| Timeout, refs, bucket coverage | Delayed-ref migration tests |
| Recall and resource policies | Reproducible benchmark matrix |

For a collation-bearing PK, the wire identity includes its name, ID,
canonical rule definition SHA-256, and ICU runtime version. The sender
and router reject absent or unequal fields before comparing any keys.
The serializer for canonical rules must be exercised against the actual
Tarantool collation catalog in the router tests. Key values remain exact MsgPack
values, including uint64 beyond 2^53.

The error classes above are API names assigned to the implementation
tests. The implementation allocates numeric Tarantool diagnostics
without collision and checks error transport through Lua and IProto.
