local clock = require('clock')
local json = require('json')
local tarantool = require('tarantool')

local work_dir = assert(os.getenv('VECTOR_BENCH_WORKDIR'))
local dimension = assert(tonumber(arg[1]))
local started = clock.monotonic()
box.cfg({work_dir = work_dir, log_level = 'error'})
local recovery_seconds = clock.monotonic() - started
local space = assert(box.space.vector_bench)
local index = assert(space.index.vec)
local query = {}
for i = 1, dimension do query[i] = 0 end
started = clock.monotonic()
local rows = index:select({query}, {
    iterator = 'neighbor', limit = 10, with_distance = true,
    timeout = 10,
})
local first_search_seconds = clock.monotonic() - started
assert(#rows > 0)
print(json.encode({
    version = tarantool.version,
    target = tarantool.build.target,
    dimension = dimension,
    tuples = space:len(),
    recovery_seconds = recovery_seconds,
    first_search_seconds = first_search_seconds,
    index_memory = index:stat().memory,
    versions = index:stat().versions,
}))
os.exit(0)
