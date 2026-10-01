local fiber = require('fiber')
local key_def = require('key_def')
local msgpack = require('msgpack')

local M = {}
local definitions = {}
local vshard_storage

local function fail(kind, reason)
    error(box.error.new({type = kind, reason = reason}), 0)
end

local function array()
    return setmetatable({}, {__serialize = 'seq'})
end

local function finite(value)
    return type(value) == 'number' and value == value and
           value ~= math.huge and value ~= -math.huge
end

local function positive_integer(value)
    return (type(value) == 'number' or type(value) == 'cdata') and
           value > 0 and value % 1 == 0
end

local function nonnegative_integer(value)
    return (type(value) == 'number' or type(value) == 'cdata') and
           value >= 0 and value % 1 == 0
end

local function check_fields(value, allowed, kind)
    if type(value) ~= 'table' then
        fail(kind, 'Expected a table')
    end
    for key in pairs(value) do
        if not allowed[key] then
            fail(kind, 'Unknown field: ' .. tostring(key))
        end
    end
end

local function array_length(value, max_count)
    if type(value) ~= 'table' then
        return nil
    end
    local count = 0
    for key in pairs(value) do
        if not nonnegative_integer(key) or key == 0 or
           key > max_count then
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

local config_fields = {
    space = true, index = true, bucket_field = true,
    bucket_index = true, pk_index = true, max_buckets = true,
    max_limit = true, max_response_bytes = true, authorize = true,
}

local request_fields = {
    name = true, query = true, L = true, scope = true,
    algorithm_opts = true,
}

local scope_fields = {
    kind = true, bucket_id = true, bucket_ids = true,
}

local algorithm_fields = {ef_search = true}

local function resolve(def)
    local space = box.space[def.space]
    if space == nil or space.id ~= def.space_id then
        fail('VECTOR_PROTOCOL', 'Configured space is unavailable')
    end
    local index = space.index[def.index]
    local pk = space.index[def.pk_index]
    local bucket_index = space.index[def.bucket_index]
    if index == nil or index.id ~= def.index_id or
       index.type ~= 'VECTOR' or pk == nil or pk.id ~= 0 or
       bucket_index == nil or bucket_index.id ~= def.bucket_index_id or
       bucket_index.parts[1].fieldno ~= def.bucket_fieldno or
       index.parts[1].fieldno ~= def.vector_fieldno then
        fail('VECTOR_PROTOCOL', 'Configured indexes have changed')
    end
    local format = space:format()
    if format[def.bucket_fieldno] == nil or
       format[def.bucket_fieldno].name ~= def.bucket_field or
       format[def.bucket_fieldno].type ~= 'unsigned' or
       #pk.parts ~= #def.pk_parts then
        fail('VECTOR_PROTOCOL', 'Configured schema has changed')
    end
    for i, part in ipairs(pk.parts) do
        local expected = def.pk_parts[i]
        if part.fieldno ~= expected.fieldno or
           part.type ~= expected.type or
           part.collation ~= expected.collation or
           part.is_nullable or part.path ~= nil then
            fail('VECTOR_PROTOCOL', 'Primary key definition has changed')
        end
    end
    local stat = index:stat().config
    if stat.dimension ~= def.dimension or stat.distance ~= def.distance then
        fail('VECTOR_PROTOCOL', 'VECTOR definition has changed')
    end
    return space, index
end

local function metadata(def)
    local parts = array()
    for i, part in ipairs(def.pk_parts) do
        parts[i] = {
            type = part.type,
            order = part.sort_order or 'asc',
            fieldno = i,
            is_nullable = false,
            collation = part.collation,
        }
    end
    return {
        version = 1,
        dimension = def.dimension,
        distance = def.distance,
        scalar = 'float32',
        numeric_contract = 'f32_f64_v1',
        pk = {parts = parts},
    }
end

local function envelope(def)
    local result = metadata(def)
    result.records = array()
    return result
end

local function remaining(deadline)
    local value = deadline - fiber.clock()
    if value <= 0 then
        fail('VECTOR_TIMEOUT', 'Storage search deadline exceeded')
    end
    return value
end

local function requested_buckets(request, context, max_buckets)
    local scope = request.scope
    check_fields(scope, scope_fields, 'VECTOR_INVALID')
    if scope.kind == 'all' then
        if scope.bucket_id ~= nil or scope.bucket_ids ~= nil then
            fail('VECTOR_INVALID', 'Conflicting scope fields')
        end
        return context:bucket_ids(max_buckets)
    end
    local wanted = array()
    if scope.kind == 'bucket' then
        if scope.bucket_ids ~= nil or
           not positive_integer(scope.bucket_id) then
            fail('VECTOR_INVALID', 'Invalid bucket scope')
        end
        wanted[1] = scope.bucket_id
    elseif scope.kind == 'buckets' then
        if scope.bucket_id ~= nil or
           array_length(scope.bucket_ids, max_buckets) == nil then
            fail('VECTOR_INVALID', 'Invalid bucket set')
        end
        for i, id in ipairs(scope.bucket_ids) do
            if not positive_integer(id) then
                fail('VECTOR_INVALID', 'Invalid bucket identifier')
            end
            wanted[i] = id
        end
    else
        fail('VECTOR_INVALID', 'Unknown bucket scope')
    end
    local result = array()
    for _, id in ipairs(wanted) do
        if context:contains(id) then
            result[#result + 1] = id
        end
    end
    if context.mode == 'bucket' and scope.kind == 'bucket' and
       #result == 0 then
        fail('VECTOR_COVERAGE', 'Bucket does not match protected call')
    end
    return result
end

local function sort_records(records, def)
    table.sort(records, function(a, b)
        if a.distance ~= b.distance then
            return a.distance < b.distance
        end
        if a.bucket_id ~= b.bucket_id then
            return def.bucket_key_def:compare_keys(
                {a.bucket_id}, {b.bucket_id}) < 0
        end
        return def.pk_key_def:compare_keys(a.id, b.id) < 0
    end)
end

local function project(rows, def, deadline)
    local records = array()
    for i, row in ipairs(rows) do
        if i % 64 == 0 then
            remaining(deadline)
        end
        local tuple = row.tuple
        local id = array()
        for j, part in ipairs(def.pk_parts) do
            id[j] = tuple[part.fieldno]
        end
        records[i] = {
            id = id,
            bucket_id = tuple[def.bucket_fieldno],
            vector = tuple[def.vector_fieldno],
            distance = row.distance,
        }
    end
    return records
end

function M.configure(name, opts)
    if box.session.euid() ~= 1 then
        fail('VECTOR_COVERAGE', 'Only admin may configure VECTOR storage')
    end
    if type(name) ~= 'string' or name == '' then
        fail('VECTOR_INVALID', 'Logical index name is required')
    end
    check_fields(opts, config_fields, 'VECTOR_INVALID')
    for _, key in ipairs({'space', 'index', 'bucket_field',
                          'bucket_index', 'pk_index'}) do
        if type(opts[key]) ~= 'string' or opts[key] == '' then
            fail('VECTOR_INVALID', 'Configuration requires ' .. key)
        end
    end
    local space = box.space[opts.space]
    if space == nil then
        fail('VECTOR_INVALID', 'Space is unavailable')
    end
    local index = space.index[opts.index]
    local pk = space.index[opts.pk_index]
    local bucket_index = space.index[opts.bucket_index]
    if index == nil or index.type ~= 'VECTOR' or
       pk == nil or pk.id ~= 0 or bucket_index == nil then
        fail('VECTOR_INVALID', 'Invalid index configuration')
    end
    local bucket_fieldno
    for i, field in ipairs(space:format()) do
        if field.name == opts.bucket_field then
            if field.type ~= 'unsigned' then
                fail('VECTOR_INVALID', 'Bucket field must be unsigned')
            end
            bucket_fieldno = i
            break
        end
    end
    if bucket_fieldno == nil or
       bucket_index.parts[1].fieldno ~= bucket_fieldno then
        fail('VECTOR_INVALID', 'Missing bucket index')
    end
    local max_buckets = opts.max_buckets or 65536
    local max_limit = opts.max_limit or 1024
    local max_response_bytes = opts.max_response_bytes or 32 * 1024 * 1024
    if not positive_integer(max_buckets) or max_buckets > 65536 or
       not positive_integer(max_limit) or max_limit > 1024 or
       not positive_integer(max_response_bytes) or
       max_response_bytes > 32 * 1024 * 1024 then
        fail('VECTOR_INVALID', 'Invalid storage limits')
    end
    if opts.authorize ~= nil and type(opts.authorize) ~= 'function' then
        fail('VECTOR_INVALID', 'authorize must be a function')
    end
    local pk_parts = array()
    local wire_parts = array()
    for i, part in ipairs(pk.parts) do
        if part.is_nullable or part.path ~= nil then
            fail('VECTOR_UNSUPPORTED', 'Unsupported primary key part')
        end
        pk_parts[i] = part
        wire_parts[i] = {
            fieldno = i, type = part.type,
            collation = part.collation,
        }
    end
    local ok, storage = pcall(require, 'vshard.storage')
    if not ok or type(storage.call_context) ~= 'function' then
        fail('VECTOR_UNSUPPORTED', 'Patched vshard storage is required')
    end
    vshard_storage = storage
    local stat = index:stat().config
    local definition = {
        space = opts.space,
        space_id = space.id,
        index = opts.index,
        index_id = index.id,
        pk_index = opts.pk_index,
        bucket_index = opts.bucket_index,
        bucket_index_id = bucket_index.id,
        bucket_fieldno = bucket_fieldno,
        bucket_field = opts.bucket_field,
        vector_fieldno = index.parts[1].fieldno,
        dimension = stat.dimension,
        distance = stat.distance,
        pk_parts = pk_parts,
        pk_key_def = key_def.new(wire_parts),
        bucket_key_def = key_def.new({{
            fieldno = 1, type = 'unsigned',
        }}),
        max_buckets = max_buckets,
        max_limit = max_limit,
        max_response_bytes = max_response_bytes,
        authorize = opts.authorize,
    }
    local global = rawget(_G, 'vector_search') or {}
    global.storage = M
    rawset(_G, 'vector_search', global)
    box.schema.func.create('vector_search.storage.search', {
        language = 'LUA', if_not_exists = true,
    })
    definitions[name] = definition
end

function M.search(request, remaining_timeout)
    check_fields(request, request_fields, 'VECTOR_INVALID')
    local def = definitions[request.name]
    if def == nil then
        fail('VECTOR_INVALID', 'Unknown logical index')
    end
    if not finite(remaining_timeout) or remaining_timeout <= 0 then
        fail('VECTOR_TIMEOUT', 'No remaining storage budget')
    end
    local deadline = fiber.clock() + math.min(remaining_timeout, 30)
    if not nonnegative_integer(request.L) or request.L > def.max_limit or
       array_length(request.query, def.dimension) ~= def.dimension then
        fail('VECTOR_INVALID', 'Invalid storage query or limit')
    end
    local algorithm_opts = request.algorithm_opts or {}
    check_fields(algorithm_opts, algorithm_fields, 'VECTOR_INVALID')
    if def.authorize ~= nil and not def.authorize(box.session.user(),
                                                   request.scope) then
        fail('VECTOR_COVERAGE', 'Caller is not authorized for this scope')
    end
    local context = vshard_storage.call_context()
    if context == nil or type(context.bucket_ids) ~= 'function' then
        fail('VECTOR_COVERAGE', 'Protected vshard call is required')
    end
    local _, index = resolve(def)
    local buckets = requested_buckets(request, context, def.max_buckets)
    local select_opts = {
        iterator = 'neighbor', limit = #buckets == 0 and 0 or request.L,
        with_distance = true, timeout = remaining(deadline),
        filter = {field = def.bucket_field, values = buckets},
    }
    if algorithm_opts.ef_search ~= nil then
        select_opts.opts = algorithm_opts
    end
    local rows = index:select({request.query}, select_opts)
    local result = envelope(def)
    result.records = project(rows, def, deadline)
    sort_records(result.records, def)
    remaining(deadline)
    if #msgpack.encode(result) > def.max_response_bytes then
        fail('VECTOR_WORK_LIMIT', 'Storage response size limit exceeded')
    end
    return result
end

return M
