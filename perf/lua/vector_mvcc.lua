local clock = require('clock')
local digest = require('digest')
local ffi = require('ffi')
local fiber = require('fiber')
local fio = require('fio')
local json = require('json')
local msgpack = require('msgpack')
local tarantool = require('tarantool')

local count = tonumber(arg[1] or 1000)
local dimension = tonumber(arg[2] or 16)
assert(count >= 100 and dimension >= 1 and dimension <= 4096)
local vectors = {}
for id = 1, count do
    local vector = {}
    for j = 1, dimension do
        vector[j] = tonumber(ffi.cast('float',
                                     ((id * 37 + j * 19) % 1000) / 500 - 1))
    end
    vectors[id] = vector
end
local checksum = digest.sha256_hex(msgpack.encode(vectors))
local work_dir = os.getenv('VECTOR_BENCH_WORKDIR') or
                 ('vector-mvcc-' .. count .. '-' .. dimension)
fio.mktree(work_dir)
box.cfg({work_dir = work_dir, log_level = 'error',
         memtx_use_mvcc_engine = true})
local space = box.schema.space.create('vector_mvcc_bench')
space:format({{name = 'id', type = 'unsigned'},
              {name = 'embedding', type = 'array'}})
space:create_index('pk')
for id, vector in ipairs(vectors) do space:insert{id, vector} end
local index = space:create_index('vec', {
    type = 'vector', dimension = dimension, distance = 'l2',
    unique = false, parts = {{2, 'array'}},
})
local before = index:stat()
local query = vectors[1]
local ready = fiber.channel(1)
local release = fiber.channel(1)
local reader = fiber.new(function()
    box.begin()
    assert(#index:select({query}, {iterator = 'neighbor', limit = 10,
                                   timeout = 10}) > 0)
    ready:put(true)
    release:get()
    box.rollback()
end)
reader:set_joinable(true)
assert(ready:get(10))
local started = clock.monotonic()
for id = 1, 100 do
    local changed = {}
    for j = 1, dimension do changed[j] = -vectors[id][j] end
    space:replace{id, changed}
end
local update_seconds = clock.monotonic() - started
local held = index:stat()
release:put(true)
assert(reader:join())
box.internal.memtx_tx_gc(1000)
local after = index:stat()
assert(held.versions.retained >= before.versions.retained)
assert(after.versions.retained <= held.versions.retained)
print(json.encode({
    version = tarantool.version,
    target = tarantool.build.target,
    count = count,
    dimension = dimension,
    checksum_sha256 = checksum,
    update_seconds = update_seconds,
    before = {versions = before.versions, slots = before.slots,
              memory = before.memory},
    held = {versions = held.versions, slots = held.slots,
            memory = held.memory},
    after = {versions = after.versions, slots = after.slots,
             memory = after.memory},
}))
os.exit(0)
