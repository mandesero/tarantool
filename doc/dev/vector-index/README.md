# Memtx VECTOR index

This branch adds a memtx VECTOR index backed by USearch HNSW. Build with
its pinned `mandesero/USearch` submodule; allocator owner, graph undo and
stop hooks are included in that submodule. Vinyl and native IProto SELECT
for distance are outside this version.

Create the VECTOR index in a memtx space whose vector field has the
`array` type. Values must be dense finite numeric arrays of exactly
the configured dimension. The index stores f32 components, computes
distance in f64, and leaves original tuple values unchanged:

```lua
local s = box.schema.space.create('documents')
s:format({{name = 'id', type = 'unsigned'},
          {name = 'embedding', type = 'array'}})
s:create_index('pk')
s:create_index('vec', {type = 'vector', dimension = 2,
                       distance = 'l2', unique = false,
                       parts = {{2, 'array'}}})
s:insert{1, {0.1, 0.2}}
local rows = s.index.vec:select({{0.1, 0.2}}, {
    iterator = 'neighbor', limit = 10, with_distance = true,
    timeout = 1, opts = {ef_search = 64},
})
-- rows[1] is {tuple = <tuple>, distance = <number>}.
```

`l2` is squared Euclidean distance. `cosine` is one minus normalized
dot product; `ip` is one minus dot product. Smaller distances rank
first. Equal distances use primary-key order. `limit` is required and
is at most 1024. A filter is an unsigned field and a finite set of
unsigned values, applied before the result limit:

```lua
local rows = s.index.vec:select({{0.1, 0.2}}, {
    iterator = 'neighbor', limit = 10,
    filter = {field = 'bucket_id', values = {1, 2}},
})
```

The second example assumes a `bucket_id` field in the space. HNSW may
normally return fewer than `limit`; an expired timeout or work bound is
an error, never a partial success. `index:stat()` reports configuration,
versions, slots, memory and search counters. `index:bsize()` excludes
tuple storage. Retained MVCC versions and graph capacity consume memtx
quota. `index:rebuild()` constructs a new generation and swaps it only
after success; plan quota for both generations. If there is not enough
headroom, schedule a maintenance drop and recreate of the index.

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
seconds. Full local bounds are in [contract.md](contract.md), and build
and measured results are in [report.md](report.md).
