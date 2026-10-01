local digest = require('digest')
local key_def = require('key_def')
local msgpack = require('msgpack')

local M = {}

local function array()
    return setmetatable({}, {__serialize = 'seq'})
end

local function canonical(value)
    if type(value) ~= 'table' then
        return value
    end
    local keys = {}
    for key in pairs(value) do
        assert(type(key) == 'string' or type(key) == 'number')
        keys[#keys + 1] = key
    end
    table.sort(keys, function(a, b)
        if type(a) ~= type(b) then
            return type(a) < type(b)
        end
        return a < b
    end)
    local result = array()
    for _, key in ipairs(keys) do
        result[#result + 1] = {key, canonical(value[key])}
    end
    return result
end

function M.collation(name)
    local tuple = box.space._collation.index.name:get{name}
    if tuple == nil then
        error('Unknown collation: ' .. name)
    end
    local definition = {tuple[4], tuple[5], canonical(tuple[6])}
    return {
        name = name,
        id = tuple[1],
        fingerprint = digest.sha256_hex(msgpack.encode(definition)),
        icu_version = tuple[4] == 'ICU' and
                      box.internal.vector_icu_version() or nil,
    }
end

function M.pk_metadata(parts)
    local result = {parts = array()}
    for i, part in ipairs(parts) do
        assert(not part.is_nullable and part.path == nil)
        result.parts[i] = {
            fieldno = i,
            type = part.type,
            order = part.sort_order or 'asc',
            scale = part.scale,
            is_nullable = false,
            collation = part.collation and M.collation(part.collation) or nil,
        }
    end
    return result
end

local function equal_collation(a, b)
    if a == nil or b == nil then
        return a == b
    end
    return type(a) == 'table' and type(b) == 'table' and
           a.name == b.name and a.id == b.id and
           a.fingerprint == b.fingerprint and
           a.icu_version == b.icu_version
end

function M.equal_pk(a, b)
    if type(a) ~= 'table' or type(b) ~= 'table' or
       type(a.parts) ~= 'table' or type(b.parts) ~= 'table' or
       #a.parts ~= #b.parts then
        return false
    end
    for i, left in ipairs(a.parts) do
        local right = b.parts[i]
        if type(right) ~= 'table' or left.fieldno ~= i or
           right.fieldno ~= i or left.type ~= right.type or
           left.order ~= right.order or
           left.scale ~= right.scale or
           left.is_nullable ~= right.is_nullable or
           not equal_collation(left.collation, right.collation) then
            return false
        end
    end
    return true
end

function M.key_def(pk)
    assert(type(pk) == 'table' and type(pk.parts) == 'table' and
           #pk.parts > 0)
    local parts = array()
    for i, part in ipairs(pk.parts) do
        assert(part.fieldno == i and part.is_nullable == false and
               (part.order == 'asc' or part.order == 'desc'))
        -- MsgPack decodes float32 key parts as Lua numbers (float64).
        -- key_def rejects the widened value, so it cannot compare IDs
        -- received from another storage. Fail during configuration.
        if part.type == 'float32' then
            error('VECTOR_PROTOCOL: float32 primary key has no wire ' ..
                  'comparator')
        end
        local name
        if part.collation ~= nil then
            assert(type(part.collation) == 'table')
            name = part.collation.name
            assert(equal_collation(part.collation, M.collation(name)))
        end
        parts[i] = {
            fieldno = i,
            type = part.type,
            sort_order = part.order,
            scale = part.scale,
            collation = name,
        }
    end
    return key_def.new(parts)
end

return M
