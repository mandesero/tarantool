## feature/core

VECTOR index statistics expose the float32/float64 numeric contract, request
limits and cumulative local search duration in nanoseconds. The slot count
previously named `reusable` is now `pending_rebuild`: retired HNSW nodes and
their dense vectors are retained until an explicit index rebuild.
