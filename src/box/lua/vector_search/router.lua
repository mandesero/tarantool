local fiber = require('fiber')
local msgpack = require('msgpack')
local wire = require('vector_search.wire')

local M = {}
local definitions = {}
local vshard_router

local function fail(kind, reason)
    error(box.error.new({type = kind, reason = reason}), 0)
end

local function array()
    return setmetatable({}, {__serialize = 'seq'})
end

local function integer(value, minimum, maximum)
    return (type(value) == 'number' or type(value) == 'cdata') and
           value >= minimum and value <= maximum and value % 1 == 0
end

local function finite(value)
    return type(value) == 'number' and value == value and
           value ~= math.huge and value ~= -math.huge
end

local function array_length(value, maximum)
    if type(value) ~= 'table' then
        return nil
    end
    local count = 0
    for key in pairs(value) do
        if not integer(key, 1, maximum) then
            return nil
        end
        count = count + 1
    end
    for i = 1, count do
        if rawget(value, i) == nil then
            return nil
        end
    end
    return count
end

local function check_fields(value, allowed)
    if type(value) ~= 'table' then
        fail('VECTOR_INVALID', 'Expected a table')
    end
    for key in pairs(value) do
        if not allowed[key] then
            fail('VECTOR_INVALID', 'Unknown field: ' .. tostring(key))
        end
    end
end

local config_fields = {
    dimension = true, distance = true, pk_parts = true,
    max_participants = true, max_pk_bytes = true,
    max_response_bytes = true,
}
local option_fields = {
    k = true, L = true, scope = true, timeout = true,
    algorithm_opts = true,
}
local scope_fields = {
    kind = true, bucket_id = true, bucket_ids = true,
}

local function remaining(deadline)
    local value = deadline - fiber.clock()
    if value <= 0 then
        fail('VECTOR_TIMEOUT', 'Router search deadline exceeded')
    end
    return value
end

local function checked_scope(scope, bucket_count)
    check_fields(scope, scope_fields)
    if scope.kind == 'all' then
        if scope.bucket_id ~= nil or scope.bucket_ids ~= nil then
            fail('VECTOR_INVALID', 'Conflicting scope fields')
        end
        return scope, bucket_count
    end
    if scope.kind == 'bucket' then
        if scope.bucket_ids ~= nil or
           not integer(scope.bucket_id, 1, bucket_count) then
            fail('VECTOR_INVALID', 'Invalid bucket scope')
        end
        return scope, 1
    end
    if scope.kind ~= 'buckets' or scope.bucket_id ~= nil then
        fail('VECTOR_INVALID', 'Invalid bucket scope')
    end
    local count = array_length(scope.bucket_ids, 65536)
    if count == nil then
        fail('VECTOR_INVALID', 'Invalid bucket set')
    end
    local seen = {}
    for _, id in ipairs(scope.bucket_ids) do
        if not integer(id, 1, bucket_count) or seen[id] then
            fail('VECTOR_INVALID', 'Invalid bucket set')
        end
        seen[id] = true
    end
    return scope, count
end

local function compare(a, b, pk_def, bucket_def)
    if a.distance ~= b.distance then
        return a.distance < b.distance and -1 or 1
    end
    local order = bucket_def:compare_keys({a.bucket_id}, {b.bucket_id})
    if order ~= 0 then
        return order
    end
    return pk_def:compare_keys(a.id, b.id)
end

local function validate_envelope(env, def, limit, bucket_count,
                                 covered, records, deadline)
    if type(env) ~= 'table' or env.version ~= 1 or
       env.dimension ~= def.dimension or
       env.distance ~= def.distance or env.scalar ~= 'float32' or
       env.numeric_contract ~= 'f32_f64_v1' or
       not wire.equal_pk(env.pk, def.pk) then
        fail('VECTOR_PROTOCOL', 'Incompatible storage envelope')
    end
    local bucket_length = array_length(env.covered_bucket_ids, 65536)
    local record_length = array_length(env.records, limit)
    if bucket_length == nil or record_length == nil then
        fail('VECTOR_PROTOCOL', 'Malformed storage arrays')
    end
    local local_buckets = {}
    for _, id in ipairs(env.covered_bucket_ids) do
        if not integer(id, 1, bucket_count) or
           local_buckets[id] or covered[id] then
            fail('VECTOR_COVERAGE', 'Duplicate or invalid bucket coverage')
        end
        local_buckets[id] = true
        covered[id] = true
    end
    local previous
    for _, record in ipairs(env.records) do
        if type(record) ~= 'table' or
           not finite(record.distance) or
           not integer(record.bucket_id, 1, bucket_count) or
           not local_buckets[record.bucket_id] or
           array_length(record.id, #def.pk.parts) ~= #def.pk.parts or
           array_length(record.vector, def.dimension) ~= def.dimension then
            fail('VECTOR_PROTOCOL', 'Invalid storage record')
        end
        local ok = pcall(def.pk_def.compare_keys, def.pk_def,
                         record.id, record.id)
        if not ok then
            fail('VECTOR_PROTOCOL', 'Invalid primary key value')
        end
        if previous ~= nil and
           compare(previous, record, def.pk_def, def.bucket_def) > 0 then
            fail('VECTOR_PROTOCOL', 'Unsorted storage records')
        end
        previous = record
        records[#records + 1] = record
        if #records % 64 == 0 then
            remaining(deadline)
        end
    end
end

local function validate_coverage(scope, bucket_count, covered, deadline)
    if scope.kind ~= 'all' then
        local expected = {}
        if scope.kind == 'bucket' then
            expected[scope.bucket_id] = true
        else
            for _, id in ipairs(scope.bucket_ids) do
                expected[id] = true
            end
        end
        for id in pairs(covered) do
            if not expected[id] then
                fail('VECTOR_COVERAGE', 'Unexpected bucket coverage')
            end
        end
    end
    if scope.kind == 'bucket' then
        if not covered[scope.bucket_id] then
            fail('VECTOR_COVERAGE', 'Requested bucket was not covered')
        end
    elseif scope.kind == 'buckets' then
        for _, id in ipairs(scope.bucket_ids) do
            if not covered[id] then
                fail('VECTOR_COVERAGE', 'Requested bucket was not covered')
            end
        end
    else
        for id = 1, bucket_count do
            if not covered[id] then
                fail('VECTOR_COVERAGE', 'Cluster bucket was not covered')
            end
            if id % 64 == 0 then
                remaining(deadline)
            end
        end
    end
end

local function validate_identities(records, def, deadline)
    table.sort(records, function(a, b)
        local order = def.bucket_def:compare_keys(
            {a.bucket_id}, {b.bucket_id})
        if order ~= 0 then
            return order < 0
        end
        return def.pk_def:compare_keys(a.id, b.id) < 0
    end)
    remaining(deadline)
    local previous
    for i, record in ipairs(records) do
        if previous ~= nil and record.bucket_id == previous.bucket_id and
           def.pk_def:compare_keys(record.id, previous.id) == 0 then
            fail('VECTOR_COVERAGE', 'Duplicate distributed identity')
        end
        previous = record
        if i % 64 == 0 then
            remaining(deadline)
        end
    end
end

local function heap_less(a, b, def)
    return compare(a.list[a.position], b.list[b.position],
                   def.pk_def, def.bucket_def) < 0
end

local function heap_push(heap, node, def)
    local position = #heap + 1
    while position > 1 do
        local parent = math.floor(position / 2)
        if not heap_less(node, heap[parent], def) then
            break
        end
        heap[position] = heap[parent]
        position = parent
    end
    heap[position] = node
end

local function heap_pop(heap, def)
    local first = heap[1]
    local last = heap[#heap]
    heap[#heap] = nil
    if #heap == 0 then
        return first
    end
    local parent = 1
    while parent * 2 <= #heap do
        local child = parent * 2
        if child + 1 <= #heap and
           heap_less(heap[child + 1], heap[child], def) then
            child = child + 1
        end
        if not heap_less(heap[child], last, def) then
            break
        end
        heap[parent] = heap[child]
        parent = child
    end
    heap[parent] = last
    return first
end

local function merge(lists, limit, def, deadline)
    local heap = {}
    local result = array()
    for _, list in ipairs(lists) do
        if #list > 0 then
            heap_push(heap, {list = list, position = 1}, def)
        end
    end
    while #heap > 0 and #result < limit do
        local node = heap_pop(heap, def)
        result[#result + 1] = node.list[node.position]
        node.position = node.position + 1
        if node.position <= #node.list then
            heap_push(heap, node, def)
        end
        if #result % 64 == 0 then
            remaining(deadline)
        end
    end
    return result
end

function M.configure(name, opts)
    if box.session.euid() ~= 1 then
        fail('VECTOR_COVERAGE', 'Only admin may configure VECTOR router')
    end
    if type(name) ~= 'string' or name == '' then
        fail('VECTOR_INVALID', 'Logical index name is required')
    end
    check_fields(opts, config_fields)
    if not integer(opts.dimension, 1, 4096) or
       (opts.distance ~= 'l2' and opts.distance ~= 'cosine' and
        opts.distance ~= 'ip') or
       array_length(opts.pk_parts, 32) == nil or #opts.pk_parts == 0 then
        fail('VECTOR_INVALID', 'Invalid router schema')
    end
    local max_participants = opts.max_participants or 128
    local max_pk_bytes = opts.max_pk_bytes or 1024
    local max_response_bytes = opts.max_response_bytes or 32 * 1024 * 1024
    if not integer(max_participants, 1, 1024) or
       not integer(max_pk_bytes, 1, 65536) or
       not integer(max_response_bytes, 1, 32 * 1024 * 1024) then
        fail('VECTOR_INVALID', 'Invalid router limits')
    end
    local ok, router = pcall(require, 'vshard.router')
    if not ok or type(router.callrw) ~= 'function' or
       type(router.map_callrw) ~= 'function' or
       type(router.bucket_count) ~= 'function' or
       type(router.info) ~= 'function' then
        fail('VECTOR_UNSUPPORTED', 'Patched vshard router is required')
    end
    local pk = wire.pk_metadata(opts.pk_parts)
    local pk_def = wire.key_def(pk)
    vshard_router = router
    definitions[name] = {
        dimension = opts.dimension,
        distance = opts.distance,
        pk = pk,
        pk_def = pk_def,
        bucket_def = require('key_def').new({{
            fieldno = 1, type = 'unsigned',
        }}),
        max_participants = max_participants,
        max_pk_bytes = max_pk_bytes,
        max_response_bytes = max_response_bytes,
    }
end

function M.search(name, query, opts)
    local def = definitions[name]
    if def == nil then
        fail('VECTOR_INVALID', 'Unknown logical index')
    end
    check_fields(opts, option_fields)
    if array_length(query, def.dimension) ~= def.dimension or
       not integer(opts.k, 0, 1024) then
        fail('VECTOR_INVALID', 'Invalid query or k')
    end
    for _, value in ipairs(query) do
        if not finite(value) then
            fail('VECTOR_INVALID', 'Query has a non-finite component')
        end
    end
    local limit = opts.L or opts.k
    local timeout = opts.timeout or 1
    if not integer(limit, opts.k, 1024) or
       not finite(timeout) or timeout <= 0 or timeout > 30 then
        fail('VECTOR_INVALID', 'Invalid L or timeout')
    end
    local algorithm_opts = opts.algorithm_opts or {}
    check_fields(algorithm_opts, {ef_search = true})
    if algorithm_opts.ef_search ~= nil and
       not integer(algorithm_opts.ef_search, 1, 8192) then
        fail('VECTOR_INVALID', 'Invalid ef_search')
    end
    local deadline = fiber.clock() + timeout
    local bucket_count = vshard_router.bucket_count()
    if not integer(bucket_count, 1, 65536) then
        fail('VECTOR_WORK_LIMIT', 'Unsupported cluster bucket count')
    end
    local scope, requested_count = checked_scope(
        opts.scope or {kind = 'all'}, bucket_count)
    if requested_count == 0 or opts.k == 0 then
        return array()
    end
    local participant_count = 1
    local info
    if scope.kind ~= 'bucket' then
        info = vshard_router.info()
        if type(info) ~= 'table' or type(info.replicasets) ~= 'table' then
            fail('VECTOR_REMOTE', 'Cannot enumerate replicasets')
        end
        participant_count = 0
        for _ in pairs(info.replicasets) do
            participant_count = participant_count + 1
        end
    end
    if participant_count == 0 or
       participant_count > def.max_participants or
       participant_count * (limit *
           (def.dimension * 9 + def.max_pk_bytes + 128) + 2048) >
           def.max_response_bytes then
        fail('VECTOR_WORK_LIMIT', 'Estimated response exceeds router limit')
    end
    local request = {
        name = name, query = query, L = limit, scope = scope,
        algorithm_opts = algorithm_opts,
    }
    local lists = array()
    local records = array()
    local covered = {}
    if scope.kind == 'bucket' then
        local ok, env, err = pcall(vshard_router.callrw,
            scope.bucket_id, 'vector_search.storage.search',
            {request, remaining(deadline)}, {timeout = remaining(deadline)})
        if not ok or env == nil then
            fail('VECTOR_REMOTE', 'Bucket call failed: ' ..
                 tostring(err or env))
        end
        if #msgpack.encode(env) > def.max_response_bytes then
            fail('VECTOR_WORK_LIMIT', 'Storage response exceeds router limit')
        end
        validate_envelope(env, def, limit, bucket_count, covered,
                          records, deadline)
        lists[1] = env.records
    else
        local ok, map, err = pcall(vshard_router.map_callrw,
            'vector_search.storage.search',
            {request, remaining(deadline)},
            {timeout = remaining(deadline), remaining_timeout_arg = 2})
        if not ok or map == nil then
            fail('VECTOR_REMOTE', 'Map call failed: ' ..
                 tostring(err or map))
        end
        if type(map) ~= 'table' then
            fail('VECTOR_PROTOCOL', 'Malformed map response')
        end
        local count = 0
        local total_bytes = 0
        for id, wrapped in pairs(map) do
            count = count + 1
            if info.replicasets[id] == nil or
               array_length(wrapped, 1) ~= 1 then
                fail('VECTOR_PROTOCOL', 'Unexpected map participant')
            end
            local env = wrapped[1]
            total_bytes = total_bytes + #msgpack.encode(env)
            if total_bytes > def.max_response_bytes then
                fail('VECTOR_WORK_LIMIT', 'Map response exceeds router limit')
            end
            validate_envelope(env, def, limit, bucket_count, covered,
                              records, deadline)
            lists[#lists + 1] = env.records
        end
        if count ~= participant_count then
            fail('VECTOR_REMOTE', 'Missing map participant')
        end
    end
    validate_coverage(scope, bucket_count, covered, deadline)
    validate_identities(records, def, deadline)
    remaining(deadline)
    local result = merge(lists, opts.k, def, deadline)
    remaining(deadline)
    return result
end

return M
