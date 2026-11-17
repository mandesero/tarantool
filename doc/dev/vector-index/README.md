# Memtx VECTOR index

This branch adds a memtx VECTOR index backed by USearch HNSW. Build with
its pinned `mandesero/USearch` submodule; allocator owner, graph undo and
stop hooks are included in that submodule. Vinyl is outside this version.
Native IProto SELECT returns tuples;
rich search with distances uses Lua or CALL.

Create the VECTOR index in a memtx space whose vector field has the
`embedding` type. Create values with `embedding.new(array)`; coordinates
are finite f32 values of the configured dimension. The index borrows their
inline tuple payload, computes distance in f64, and retains the source tuple:

```lua
local embedding = require('embedding')
local s = box.schema.space.create('documents')
s:format({{name = 'id', type = 'unsigned'},
          {name = 'embedding', type = 'embedding'}})
s:create_index('pk')
s:create_index('vec', {type = 'vector', dimension = 2,
                       distance = 'l2', unique = false,
                       parts = {{2, 'embedding'}}})
s:insert{1, embedding.new({0.1, 0.2})}
local tuples, _, distances = s.index.vec:search({0.1, 0.2}, {
    limit = 10, with_distance = true,
    timeout = 1, opts = {ef_search = 64},
})
-- distances[i] belongs to tuples[i].
local rows = s.index.vec:select({{0.1, 0.2}}, {
    iterator = 'neighbor', limit = 10,
})
-- The same tuple-only SELECT works through net.box.
```

`l2` is squared Euclidean distance. `cosine` is one minus normalized
dot product; `ip` is one minus dot product. Smaller distances rank
first. Equal distances use primary-key order. `limit` is required and
is at most 1024. A filter is an unsigned field and a finite set of
unsigned values, applied before the result limit:

```lua
local rows = s.index.vec:search({0.1, 0.2}, {
    limit = 10,
    filter = {field = 'bucket_id', values = {1, 2}},
})
```

The second example assumes a `bucket_id` field in the space. HNSW may
normally return fewer than `limit`; an expired timeout or work bound is
an error, never a partial success. `index:stat()` reports configuration,
versions, slots, memory and search counters. `index:bsize()` excludes
tuple storage and process RSS. Graph/vector allocations use malloc charged
to memtx quota, rather than the slab arena. `memory.generations.current`
and `memory.generations.transient` distinguish the active generation from
one being built or reclaimed; `memory.total` includes both. Retained MVCC
versions and graph capacity consume memtx
quota. Payload-only updates share the existing vector and graph node while
keeping independent tuple visibility. Rebuild shares coordinates between its
two graphs and transfers concurrent vector changes to both. DML and tuple GC
do not restart it; index replacement or DROP still returns `MODIFY_INDEX`.
Searches keep using the current generation, and publication is atomic.

A background fiber compacts navigation tombstones automatically. Manual
`index:rebuild()` waits for completion and is forbidden inside a transaction.
Maintenance holds reserved memtx quota. While it builds, new embeddings have
a fixed admission budget; exhaustion returns OutOfMemory without aborting the
builder. Long transactions may retain old versions; quota limits still apply.
`memory.maintenance_reserved` reports unused quota separately from allocated
bytes. Build and cleanup yield with a 1 ms target between graph operations,
not a strict latency guarantee for each operation. Online creation on a
nonempty space with a HASH primary key remains rejected.

## Traversal and embedding values

Ordinary VECTOR `pairs` is unsupported. Use the space or primary index for
ALL traversal; `index:count()` counts visible rows through primary storage.
Keyed and NEIGHBOR count are unsupported. Bounded `select` and `search`
provide HNSW top-k. The backend does not expose a resumable ANN cursor.
Public `box.read_view` independently uses frozen primary storage for exact
ALL/NEIGHBOR scans that survive DML, compaction and DROP. Ordered read-view
scans retain at most 32 next results and rescan for every batch.

The `embedding` module provides `new(array)`, `is(value)`, `totable(value)`
and `#value`. MsgPack EXT type 9 stores d little-endian finite f32 components;
this initial wire format has no payload header. ARRAY and BIN fields are not
accepted by VECTOR; queries may be arrays or embedding values. Navigation
references can retain the entire source tuple after version GC.

After the last binding is collected, a slot is reusable. New insertions
replace its coordinates and links with allocation-free rollback on failure.
Old navigation coordinates are released after the successful replacement.
`slots.reusable` counts these slots; rebuilding physically compacts them.
A graph is limited to `2 * K + 64` nodes, with K the historical peak number
of embeddings needed by retained tuple versions. At this limit reuse is
allowed, while append fails with OutOfMemory. `slots.nodes`,
`slots.node_limit` and `slots.embedding_peak` report these values.

## Distributed search

Install the companion
[CRUD branch](https://github.com/mandesero/crud/tree/codex/vector-search)
and initialize its ordinary router/storage roles. The public API is
`crud.vector_search(space_name, index_name, query, opts)`; see its
[guide](https://github.com/mandesero/crud/blob/codex/vector-search/doc/vector-search.md).
It requires the protected-call support in the companion vshard fork.
The distributed Lua package, wire validation and cluster fixture live
in CRUD. No vector storage role is configured in Tarantool.

## Operations and errors

`VECTOR_INVALID` reports invalid input, `VECTOR_UNSUPPORTED` an unsupported
operation, `VECTOR_TIMEOUT` an expired budget, and `VECTOR_WORK_LIMIT` a
work or size bound. Allocation failures use the standard OutOfMemory
diagnostic. The local timeout defaults to one second and is at most 30
seconds for `search()`. Ordinary SELECT uses the one-second server budget;
`net.box`'s `timeout` controls transport waiting. ANN options belong only to
`search()` and are rejected by ordinary local SELECT. Full local bounds are in
[contract.md](contract.md), and build
and measured results are in [report.md](report.md).
