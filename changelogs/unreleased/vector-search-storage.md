## Feature

- Add a configurable VECTOR storage procedure for vshard protected calls.
  It limits results to the requested live buckets, returns a versioned
  envelope through IProto CALL, and enforces a relative search budget.
- Add a VECTOR router search procedure that checks bucket coverage and
  schema compatibility and merges bounded candidates from masters.
  Fixed decimal PK scale is part of the wire contract. A float32 PK is
  rejected because its values cannot be compared after MsgPack decoding.
