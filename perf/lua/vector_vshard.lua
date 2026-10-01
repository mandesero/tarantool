local clock = require('clock')
local digest = require('digest')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpack')

local output = assert(os.getenv('VSHARD_VECTOR_BENCH_OUTPUT'))
local vrouter = require('vshard.router')
local budget = require('vshard.router.map_budget')
local search = require('vector_search.router')
local inserted = {}
for id = 1000, 1299 do
    local uniform = id < 1150
    local bucket_id = uniform and (id * 37) % 3000 + 1 or 1
    local vector = {uniform and 100 or -100, id / 1000}
    local tuple = {id, bucket_id, vector}
    assert(vrouter.callrw(bucket_id, 'space_insert',
                          {'vector_docs', tuple}))
    inserted[#inserted + 1] = tuple
end
local checksum = digest.sha256_hex(msgpack.encode(inserted))
local participants = 0
for _ in pairs(vrouter.info().replicasets) do
    participants = participants + 1
end

local original_map = vrouter.map_callrw
local original_bucket = vrouter.callrw
local original_prepare = budget.prepare
local current
budget.prepare = function(args, remaining, position)
    if current ~= nil and position ~= nil then
        current.ref_seconds = args[position] - remaining
    end
    return original_prepare(args, remaining, position)
end
vrouter.map_callrw = function(...)
    local started = clock.monotonic()
    local result, err = original_map(...)
    current.dispatch_seconds = clock.monotonic() - started
    if result ~= nil then
        current.reply_bytes = #msgpack.encode(result)
    end
    return result, err
end
vrouter.callrw = function(...)
    local started = clock.monotonic()
    local result, err = original_bucket(...)
    current.dispatch_seconds = clock.monotonic() - started
    if result ~= nil then
        current.reply_bytes = #msgpack.encode(result)
    end
    return result, err
end

local function percentile(values, percent)
    table.sort(values)
    return values[math.max(1, math.ceil(#values * percent / 100))]
end

local function summarize(samples, field, multiplier)
    local values = {}
    for _, sample in ipairs(samples) do
        values[#values + 1] = (sample[field] or 0) * multiplier
    end
    return {
        p50 = percentile(values, 50),
        p95 = percentile(values, 95),
        p99 = percentile(values, 99),
    }
end

local function profile(name, query_x, L, scope)
    local samples = {}
    for i = 1, 30 do
        current = {}
        local started = clock.monotonic()
        local rows = search.search('docs', {query_x, 1 + i / 100}, {
            k = 5, L = L, scope = scope, timeout = 5,
            algorithm_opts = {ef_search = 128},
        })
        current.total_seconds = clock.monotonic() - started
        current.merge_seconds = current.total_seconds -
                                current.dispatch_seconds
        current.returned = #rows
        samples[#samples + 1] = current
    end
    current = nil
    return {
        name = name,
        L = L,
        scope = scope,
        samples = #samples,
        total_ms = summarize(samples, 'total_seconds', 1000),
        dispatch_ms = summarize(samples, 'dispatch_seconds', 1000),
        ref_ms = summarize(samples, 'ref_seconds', 1000),
        merge_and_validation_ms = summarize(samples,
                                             'merge_seconds', 1000),
        response_bytes = summarize(samples, 'reply_bytes', 1),
        returned = summarize(samples, 'returned', 1),
    }
end

local gc_before_kb = collectgarbage('count')
local profiles = {
    profile('uniform_all_L10', 100, 10, {kind = 'all'}),
    profile('skew_all_L10', -100, 10, {kind = 'all'}),
    profile('skew_all_L20', -100, 20, {kind = 'all'}),
    profile('single_bucket', -100, 10,
            {kind = 'bucket', bucket_id = 1}),
    profile('bucket_set', 100, 10,
            {kind = 'buckets', bucket_ids = {1, 1001, 2001}}),
}
local gc_after_kb = collectgarbage('count')
budget.prepare = original_prepare
vrouter.map_callrw = original_map
vrouter.callrw = original_bucket

local report = {
    context = {
        seed = 20260930,
        dataset = '150 uniform bucket IDs and 150 bucket 1 IDs',
        checksum_sha256 = checksum,
        participants = participants,
        k = 5,
        dimension = 2,
        distance = 'l2',
        ef_search = 128,
    },
    router_gc_kb = {before = gc_before_kb, after = gc_after_kb},
    profiles = profiles,
}
local file = assert(fio.open(output, {'O_WRONLY', 'O_CREAT', 'O_TRUNC'}))
file:write(json.encode(report), '\n')
file:close()
return true
