# VECTOR search on memtx and vshard

This implementation adds a memtx VECTOR index backed by USearch HNSW.
The local API uses `NEIGHBOR` selection. The `vector_search` Lua package
adds protected storage calls and a router merge for vshard. Vinyl,
native IProto SELECT for distance, and a cluster-wide snapshot are out
of scope for this version.

## Install and configure

Build this Tarantool branch with its pinned USearch submodule from
`mandesero/USearch`. The owner-context, graph-undo, and stop changes
are included in that submodule commit. The build embeds
`vector_search.storage`, `vector_search.router`, and their
wire helper in the Tarantool binary; there is no separate Lua rock to
install. The three-replicaset example requires the companion vshard
fork commit `674cda708d957cc145aa4f0622804bb5c2f8007a`.
Router and storage configuration reject vshard without its call-context
API.

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

## Sharded search

On each storage master, create the same space format and VECTOR index,
including an unsigned `bucket_id` field and an ordinary index over it.
Configure the protected procedure as admin after vshard is configured:

```lua
require('vector_search.storage').configure('documents', {
    space = 'documents', index = 'vec', pk_index = 'pk',
    bucket_field = 'bucket_id', bucket_index = 'bucket_id',
})
```

Grant the caller both `execute` on function
`vector_search.storage.search` and `read` on the space. An optional
`authorize` callback can further restrict a caller's bucket scope.
The filter in a request does not grant access.

On the router, configure the expected schema as admin. Its PK parts
must match the storage primary key, including type, order, decimal
scale, and collation. The `float32` PK type is rejected because its
wire values cannot be compared after MsgPack decoding:

```lua
local router = require('vector_search.router')
router.configure('documents', {
    dimension = 2, distance = 'l2',
    pk_parts = {{type = 'unsigned'}},
})
local rows = router.search('documents', {0.1, 0.2}, {
    k = 10, L = 20, timeout = 1,
    scope = {kind = 'all'},
    algorithm_opts = {ef_search = 64},
})
-- Each row is {id = {...}, bucket_id = n, vector = {...}, distance = n}.
```

Use `{kind = 'bucket', bucket_id = n}` for one bucket, or
`{kind = 'buckets', bucket_ids = {n, ...}}` for a set. An empty set
returns an empty array. `L` defaults to `k` and is the maximum number
of candidates from each storage, not a guarantee of k global results.
The router uses masters and full map calls for all-bucket and bucket-set
searches, so latency, network traffic and router memory grow with the
number of replicasets, L and dimension. It rejects incomplete coverage,
failed storage calls and incompatible metadata rather than returning a
partial result. During concurrent DML there is no single cluster
snapshot; each storage result still obeys its local visibility rules.

## Operations and errors

Keep space formats, VECTOR definitions, primary keys, collations and
module versions aligned across storages and router. Reconfigure after
schema changes. A protocol or coverage mismatch fails the query until
the deployment is consistent. `VECTOR_INVALID` reports invalid input,
`VECTOR_UNSUPPORTED` an unsupported operation, `VECTOR_TIMEOUT` an
expired budget, and `VECTOR_WORK_LIMIT` a work or size bound. The Lua
package also reports `VECTOR_PROTOCOL`, `VECTOR_COVERAGE`, and
`VECTOR_REMOTE`. Allocation errors use the standard Tarantool error.

The default local and router timeout is one second; the maximum is
30 seconds. Both k and L are at most 1024. Response size is capped at
32 MiB by default. The full bounds, numeric contract and unsupported
operations are in [contract.md](contract.md). Build, cluster verification,
and measured results are summarized in [report.md](report.md).
