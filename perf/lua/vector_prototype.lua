local bit = require('bit')
local clock = require('clock')
local digest = require('digest')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpack')
local tarantool = require('tarantool')

local count = tonumber(arg[1] or 1000)
local dimension = tonumber(arg[2] or 16)
local query_count = tonumber(arg[3] or 100)
local seed = tonumber(arg[4] or 20260930)
local k = tonumber(arg[5] or 10)
assert(count >= k and dimension >= 1 and dimension <= 4096)
assert(query_count >= 1 and k >= 1 and k <= 32)
local state = seed
local function random()
    state = bit.bxor(state, bit.lshift(state, 13))
    state = bit.bxor(state, bit.rshift(state, 17))
    state = bit.bxor(state, bit.lshift(state, 5))
    return bit.band(state, 0x7fffffff) / 2147483648
end
local vectors = {}
local queries = {}
for i = 1, count do
    local vector = {}
    for j = 1, dimension do vector[j] = random() * 2 - 1 end
    vectors[i] = vector
end
for i = 1, query_count do
    local query = {}
    for j = 1, dimension do query[j] = random() * 2 - 1 end
    queries[i] = query
end
local checksum = digest.sha256_hex(msgpack.encode({vectors, queries}))

local function cosine(a, b)
    local dot = 0
    local norm_a = 0
    local norm_b = 0
    for j = 1, dimension do
        dot = dot + a[j] * b[j]
        norm_a = norm_a + a[j] * a[j]
        norm_b = norm_b + b[j] * b[j]
    end
    return 1 - dot / math.sqrt(norm_a * norm_b)
end

local function exact(query)
    local result = {}
    for id, vector in ipairs(vectors) do
        result[#result + 1] = {id = id,
                               distance = cosine(query, vector)}
    end
    table.sort(result, function(a, b)
        if a.distance ~= b.distance then
            return a.distance < b.distance
        end
        return a.id < b.id
    end)
    return result
end

local function percentile(values, percent)
    table.sort(values)
    return values[math.max(1, math.ceil(#values * percent / 100))]
end

local work_dir = os.getenv('VECTOR_BENCH_WORKDIR') or
                 ('vector-prototype-' .. count .. '-' .. dimension)
fio.mktree(work_dir)
box.cfg({work_dir = work_dir, log_level = 'error'})
local space = box.schema.space.create('prototype_bench')
space:format({{name = 'id', type = 'unsigned'},
              {name = 'embedding', type = 'array'}})
space:create_index('pk')
for id, vector in ipairs(vectors) do space:insert{id, vector} end
local started = clock.monotonic()
local index = space:create_index('vec', {
    type = 'vector', dimension = dimension,
    unique = false, parts = {{2, 'array'}},
})
local build_seconds = clock.monotonic() - started
local times = {}
local hits = 0
local short = 0
for i, query in ipairs(queries) do
    local truth = exact(query)
    local expected = {}
    for j = 1, k do expected[truth[j].id] = true end
    started = clock.monotonic()
    local rows = index:select({query}, {iterator = 'EQ', limit = k})
    times[i] = clock.monotonic() - started
    if #rows < k then short = short + 1 end
    for _, row in ipairs(rows) do
        if expected[row[1]] then hits = hits + 1 end
    end
end
local sum = 0
for _, value in ipairs(times) do sum = sum + value end
print(json.encode({
    context = {
        version = tarantool.version,
        target = tarantool.build.target,
        algorithm = 'prototype USearch',
        scalar = 'float64',
        distance = 'cosine',
        iterator = 'EQ',
        max_candidates = 32,
        seed = seed,
        checksum_sha256 = checksum,
        count = count,
        dimension = dimension,
        query_count = query_count,
        k = k,
    },
    quality = {recall_at_k = hits / (query_count * k),
               short_fraction = short / query_count},
    search = {
        p50_ms = percentile(times, 50) * 1000,
        p95_ms = percentile(times, 95) * 1000,
        p99_ms = percentile(times, 99) * 1000,
        throughput_per_s = query_count / sum,
    },
    build_seconds = build_seconds,
    index_bsize = index:bsize(),
}))
os.exit(0)
