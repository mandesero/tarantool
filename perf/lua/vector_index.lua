local bit = require('bit')
local clock = require('clock')
local digest = require('digest')
local ffi = require('ffi')
local fiber = require('fiber')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpack')
local tarantool = require('tarantool')

local count = tonumber(arg[1] or 1000)
local dimension = tonumber(arg[2] or 8)
local query_count = tonumber(arg[3] or 100)
local seed = tonumber(arg[4] or 20260930)
local k = tonumber(arg[5] or 10)
local bucket_count = tonumber(arg[6] or 10)
assert(count and count >= math.max(k, 100) and count <= 100000)
assert(dimension and dimension >= 1 and dimension <= 4096)
assert(query_count and query_count >= 1 and query_count <= 10000)
assert(seed and seed > 0 and seed < 2147483648)
assert(k and k >= 1 and k <= 1024)
assert(bucket_count and bucket_count >= 1 and bucket_count <= 1000)

local state = seed
local function random()
    state = bit.bxor(state, bit.lshift(state, 13))
    state = bit.bxor(state, bit.rshift(state, 17))
    state = bit.bxor(state, bit.lshift(state, 5))
    return bit.band(state, 0x7fffffff) / 2147483648
end

local function f32(value)
    return tonumber(ffi.cast('float', value))
end

local vectors = {}
local queries = {}
for i = 1, count do
    local vector = {}
    for j = 1, dimension do
        vector[j] = f32(random() * 2 - 1)
    end
    vectors[i] = vector
end
for i = 1, query_count do
    local query = {}
    for j = 1, dimension do
        query[j] = f32(random() * 2 - 1)
    end
    queries[i] = query
end
local checksum = digest.sha256_hex(msgpack.encode({vectors, queries}))

local function percentile(values, percent)
    table.sort(values)
    return values[math.max(1, math.ceil(#values * percent / 100))]
end

local function distance(a, b)
    local result = 0
    for j = 1, dimension do
        local delta = a[j] - b[j]
        result = result + delta * delta
    end
    return result
end

local function exact(query, filter)
    local result = {}
    for id, vector in ipairs(vectors) do
        if filter == nil or id % bucket_count == filter then
            result[#result + 1] = {id = id,
                                   distance = distance(query, vector)}
        end
    end
    table.sort(result, function(a, b)
        if a.distance ~= b.distance then
            return a.distance < b.distance
        end
        return a.id < b.id
    end)
    return result
end

local function peak_rss_kb()
    local file = io.open('/proc/self/status')
    if file == nil then return nil end
    local content = file:read('*a')
    file:close()
    return tonumber(content:match('VmHWM:%s*(%d+)%s*kB'))
end

local work_dir = os.getenv('VECTOR_BENCH_WORKDIR') or
                 ('vector-index-' .. count .. '-' .. dimension)
fio.mktree(work_dir)
box.cfg({work_dir = work_dir, log_level = 'error'})
local space = box.schema.space.create('vector_bench')
space:format({{name = 'id', type = 'unsigned'},
              {name = 'embedding', type = 'array'},
              {name = 'bucket_id', type = 'unsigned'}})
space:create_index('pk')
for id, vector in ipairs(vectors) do
    space:insert{id, vector, id % bucket_count}
end
local started = clock.monotonic()
local index = space:create_index('vec', {
    type = 'vector', dimension = dimension, distance = 'l2',
    unique = false, parts = {{2, 'array'}},
})
local build_seconds = clock.monotonic() - started
local memory_after_build = index:stat().memory

local ann_times = {}
local exact_times = {}
local pk_times = {}
local scheduled_pk_baseline = {}
local scheduled_pk_with_ann = {}
local short = 0
local hits = 0
local possible_hits = 0
local heartbeat_max = 0
local running = true
for i = 1, math.min(100, query_count) do
    local due = clock.monotonic() + 0.001
    fiber.sleep(0.001)
    assert(space.index.pk:get{(i - 1) % count + 1} ~= nil)
    scheduled_pk_baseline[i] = math.max(0, clock.monotonic() - due)
end
local heartbeat_last = clock.monotonic()
fiber.create(function()
    while running do
        local due = clock.monotonic() + 0.001
        fiber.sleep(0.001)
        local now = clock.monotonic()
        heartbeat_max = math.max(heartbeat_max, now - heartbeat_last)
        heartbeat_last = now
        assert(space.index.pk:get{1} ~= nil)
        scheduled_pk_with_ann[#scheduled_pk_with_ann + 1] =
            math.max(0, clock.monotonic() - due)
    end
end)

for i, query in ipairs(queries) do
    local filter = i % 4 == 0 and i % bucket_count or nil
    local start = clock.monotonic()
    local truth = exact(query, filter)
    exact_times[i] = clock.monotonic() - start
    local opts = {iterator = 'neighbor', limit = k,
                  with_distance = true, timeout = 10,
                  opts = {ef_search = 128}}
    if filter ~= nil then
        opts.filter = {field = 'bucket_id', values = {filter}}
    end
    start = clock.monotonic()
    local rows = index:select({query}, opts)
    ann_times[i] = clock.monotonic() - start
    if #rows < k then short = short + 1 end
    local expected = {}
    for j = 1, math.min(k, #truth) do
        expected[truth[j].id] = true
        possible_hits = possible_hits + 1
    end
    for _, row in ipairs(rows) do
        if expected[row.tuple[1]] then hits = hits + 1 end
    end
    start = clock.monotonic()
    assert(space.index.pk:get{(i - 1) % count + 1} ~= nil)
    pk_times[i] = clock.monotonic() - start
    fiber.yield()
end
running = false
fiber.sleep(0.005)

local function timings(values)
    local sum = 0
    for _, value in ipairs(values) do sum = sum + value end
    return {
        p50_ms = percentile(values, 50) * 1000,
        p95_ms = percentile(values, 95) * 1000,
        p99_ms = percentile(values, 99) * 1000,
        throughput_per_s = #values / sum,
    }
end

local insert_start = clock.monotonic()
for id = count + 1, count + 100 do
    space:insert{id, vectors[id - count], id % bucket_count}
end
local insert_seconds = clock.monotonic() - insert_start
local update_start = clock.monotonic()
for id = 1, 100 do
    space:update({id}, {{'=', 2, vectors[count - id + 1]}})
end
local update_seconds = clock.monotonic() - update_start
local delete_start = clock.monotonic()
for id = count + 1, count + 100 do
    space:delete{id}
end
local delete_seconds = clock.monotonic() - delete_start
local memory_after_dml = index:stat().memory
local rebuild_start = clock.monotonic()
fiber.set_slice(120)
local rebuild_ok, rebuild_error = pcall(index.rebuild, index)
local rebuild_seconds = clock.monotonic() - rebuild_start
fiber.set_slice(1)
local snapshot_start = clock.monotonic()
box.snapshot()
local snapshot_seconds = clock.monotonic() - snapshot_start

local report = {
    context = {
        version = tarantool.version,
        target = tarantool.build.target,
        flags = tarantool.build.flags,
        algorithm = 'hnsw',
        distance = 'l2',
        scalar = 'float32',
        oracle = 'Lua exact scan over the same float32 vectors',
        seed = seed,
        checksum_sha256 = checksum,
        count = count,
        dimension = dimension,
        query_count = query_count,
        k = k,
        ef_search = 128,
        filter_every_nth_query = 4,
        bucket_count = bucket_count,
        filter_selectivity = 1 / bucket_count,
    },
    quality = {
        recall_at_k = hits / possible_hits,
        short_fraction = short / query_count,
    },
    search = {
        ann = timings(ann_times),
        exact_lua = timings(exact_times),
        pk_get = timings(pk_times),
        scheduled_pk_baseline = timings(scheduled_pk_baseline),
        scheduled_pk_with_ann = timings(scheduled_pk_with_ann),
        heartbeat_max_ms = heartbeat_max * 1000,
    },
    lifecycle = {
        build_seconds = build_seconds,
        rebuild_seconds = rebuild_seconds,
        rebuild_ok = rebuild_ok,
        rebuild_error = not rebuild_ok and tostring(rebuild_error) or nil,
        snapshot_seconds = snapshot_seconds,
        inserts_per_s = 100 / insert_seconds,
        updates_per_s = 100 / update_seconds,
        deletes_per_s = 100 / delete_seconds,
        memory_after_build = memory_after_build,
        memory_after_dml = memory_after_dml,
        memory_after_rebuild = index:stat().memory,
        versions_after_rebuild = index:stat().versions,
        peak_rss_kb = peak_rss_kb(),
    },
}
print(json.encode(report))
os.exit(0)
