## feature/core

VECTOR index statistics expose the float32/float64 numeric contract, request
limits and cumulative local search duration in nanoseconds. `slots.reusable`
counts GC-safe graph slots. Insertions reuse these slots with transactional
undo before appending new nodes; rebuild can physically compact them.
