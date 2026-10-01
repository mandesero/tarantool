local test_run = require('test_run').new()
local fiber = require('fiber')
local rs1 = {'storage_1_a', 'storage_1_b'}
local rs2 = {'storage_2_a', 'storage_2_b'}
local rs3 = {'storage_3_a'}

local function eval(name, source)
    source = source:gsub('\n', ' ')
    local ok, result = pcall(test_run.eval, test_run, name, source)
    assert(ok, name .. ': ' .. tostring(result))
    assert(result[1] == true, name .. ': ' .. tostring(result[1]) ..
           ' in ' .. source:sub(1, 80))
end

local setup = [[
    local s = box.schema.space.create('vector_docs', {if_not_exists = true})
    s:format({{name = 'id', type = 'unsigned'},
              {name = 'bucket_id', type = 'unsigned'},
              {name = 'embedding', type = 'array'}})
    s:create_index('pk', {if_not_exists = true})
    s:create_index('bucket_id', {if_not_exists = true, unique = false,
                                 parts = {{2, 'unsigned'}}})
    s:create_index('vec', {if_not_exists = true, type = 'vector',
                           dimension = 2, distance = 'l2',
                           unique = false, parts = {{3, 'array'}}})
    require('vector_search.storage').configure('docs', {
        space = 'vector_docs', index = 'vec', pk_index = 'pk',
        bucket_field = 'bucket_id', bucket_index = 'bucket_id',
    })
    return true
]]

local search = [[
    local router = require('vector_search.router')
    router.configure('docs', {dimension = 2, distance = 'l2',
                              pk_parts = {{fieldno = 1,
                                           type = 'unsigned'}}})
    assert(vshard.router.callrw(1, 'space_insert',
           {'vector_docs', {1, 1, {1, 0}}}))
    assert(vshard.router.callrw(1001, 'space_insert',
           {'vector_docs', {2, 1001, {0.9, 0.1}}}))
    assert(vshard.router.callrw(2001, 'space_insert',
           {'vector_docs', {3, 2001, {0, 1}}}))
    local result = router.search('docs', {1, 0},
                                 {k = 3, L = 3, timeout = 5})
    assert(#result == 3 and result[1].id[1] == 1 and
           result[2].id[1] == 2 and result[3].id[1] == 3)
    result = router.search('docs', {1, 0}, {k = 3, L = 3,
        scope = {kind = 'bucket', bucket_id = 1001}, timeout = 5})
    assert(#result == 1 and result[1].id[1] == 2)
    result = router.search('docs', {1, 0}, {k = 3, L = 3,
        scope = {kind = 'buckets', bucket_ids = {1, 2001}}, timeout = 5})
    assert(#result == 2 and result[1].id[1] == 1 and
           result[2].id[1] == 3)
    result = router.search('docs', {1, 0}, {k = 3, L = 3,
        scope = {kind = 'buckets', bucket_ids = {}}, timeout = 5})
    assert(#result == 0)
    return true
]]

local after_move = [[
    local result = require('vector_search.router').search(
        'docs', {1, 0}, {k = 3, L = 3, timeout = 5})
    assert(#result == 3 and result[1].id[1] == 1 and
           result[2].id[1] == 2 and result[3].id[1] == 3)
    return true
]]

local function run()
    test_run:create_cluster(rs1, 'router')
    test_run:create_cluster(rs2, 'router')
    test_run:create_cluster(rs3, 'router')
    local util = require('util')
    util.wait_master(test_run, rs1, 'storage_1_a')
    util.wait_master(test_run, rs2, 'storage_2_a')
    for _, name in ipairs({'storage_1_a', 'storage_1_b',
                           'storage_2_a', 'storage_2_b',
                           'storage_3_a'}) do
        local ready = false
        for _ = 1, 100 do
            local ok, result = pcall(test_run.eval, test_run, name,
                "return type(rawget(_G, 'bootstrap_storage')) " ..
                "== 'function'")
            if ok and result[1] == true then
                ready = true
                break
            end
            fiber.sleep(0.05)
        end
        assert(ready, name .. ' did not load bootstrap_storage')
        test_run:eval(name, "rawget(_G, 'bootstrap_storage')()")
    end
    util.push_rs_filters(test_run)
    test_run:cmd("create server router_1 with script='router/router_1.lua'")
    test_run:cmd('start server router_1')
    eval('router_1', 'return vshard.router.bootstrap({timeout = 10})')
    for _, name in ipairs({'storage_1_a', 'storage_2_a',
                           'storage_3_a'}) do
        eval(name, setup)
    end
    eval('router_1', search)
    local benchmark_script = os.getenv('VSHARD_VECTOR_BENCH_SCRIPT')
    if benchmark_script ~= nil then
        assert(not benchmark_script:find("'", 1, true))
        eval('router_1', "return dofile('" .. benchmark_script .. "')")
    end
    eval('storage_1_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(request, budget)
            assert(budget > 0 and budget < 0.9)
            return storage.saved_search(request, budget)
        end
        local sched = require('vshard.storage.sched')
        sched.move_start(10)
        require('fiber').create(function()
            require('fiber').sleep(0.2)
            sched.move_end(1)
        end)
        return true
    ]])
    eval('router_1', [[
        local result = require('vector_search.router').search(
            'docs', {1, 0}, {k = 3, L = 3, timeout = 1})
        return #result == 3
    ]])
    eval('storage_1_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', [[
        for id = 10, 29 do
            assert(vshard.router.callrw(1001, 'space_insert',
                   {'vector_docs', {id, 1001, {9, id}}}))
        end
        local result = require('vector_search.router').search(
            'docs', {1, 0}, {k = 1, L = 2, timeout = 5,
                            algorithm_opts = {ef_search = 4}})
        assert(#result == 1 and result[1].id[1] == 1)
        return true
    ]])
    eval('router_1', [=[
        local fiber = require('fiber')
        local ffi = require('ffi')
        local router = require('vector_search.router')
        local writer = fiber.new(function()
            for id = 30, 49 do
                assert(vshard.router.callrw(1001, 'space_insert',
                       {'vector_docs', {id, 1001, {10, id}}}))
            end
        end)
        writer:set_joinable(true)
        for _ = 1, 10 do
            local result = router.search('docs', {1, 0},
                                         {k = 3, L = 3, timeout = 5})
            for _, row in ipairs(result) do
                local x = tonumber(ffi.cast('float', row.vector[1]))
                local y = tonumber(ffi.cast('float', row.vector[2]))
                local distance = (x - 1) ^ 2 + y ^ 2
                assert(math.abs(row.distance - distance) < 1e-6)
            end
        end
        local ok, err = writer:join()
        assert(ok, tostring(err))
        local candidates = {
            {id = 1, vector = {1, 0}},
            {id = 2, vector = {0.9, 0.1}},
            {id = 3, vector = {0, 1}},
        }
        for id = 10, 29 do
            candidates[#candidates + 1] = {id = id, vector = {9, id}}
        end
        for id = 30, 49 do
            candidates[#candidates + 1] = {id = id, vector = {10, id}}
        end
        for _, query in ipairs({{1, 0}, {8, 15}, {0, 1}}) do
            local exact = {}
            for _, row in ipairs(candidates) do
                local x = tonumber(ffi.cast('float', row.vector[1]))
                local y = tonumber(ffi.cast('float', row.vector[2]))
                exact[#exact + 1] = {
                    id = row.id,
                    distance = (x - query[1]) ^ 2 +
                               (y - query[2]) ^ 2,
                }
            end
            table.sort(exact, function(a, b)
                if a.distance ~= b.distance then
                    return a.distance < b.distance
                end
                return a.id < b.id
            end)
            local actual = router.search('docs', query, {
                k = 5, L = 10, timeout = 5,
                algorithm_opts = {ef_search = 128},
            })
            local expected = {}
            for i = 1, 5 do expected[exact[i].id] = true end
            local hits = 0
            for _, row in ipairs(actual) do
                if expected[row.id[1]] then hits = hits + 1 end
            end
            assert(hits >= 4)
        end
        return true
    ]=])
    local names = {'storage_1_a', 'storage_2_a', 'storage_3_a'}
    local source
    for i, name in ipairs(names) do
        local result = test_run:eval(name,
            'return box.space._bucket:get{1} ~= nil')
        if result[1] then
            assert(source == nil)
            source = i
        end
    end
    assert(source ~= nil)
    local target = source % #names + 1
    eval(names[source], "return vshard.storage.bucket_send(1, '" ..
         util.replicasets[target] .. "', {timeout = 10})")
    eval(names[source],
         'box.space.vector_docs:insert{99, 1, {1, 0}} return true')
    eval('router_1', after_move)
    local ref_source
    for i, name in ipairs(names) do
        local result = test_run:eval(name,
            'return box.space._bucket:get{2001} ~= nil')
        if result[1] then
            assert(ref_source == nil)
            ref_source = i
        end
    end
    assert(ref_source ~= nil)
    local ref_target = ref_source % #names + 1
    eval(names[ref_source], [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(...)
            rawset(_G, 'vector_ref_entered', true)
            require('fiber').sleep(0.3)
            return storage.saved_search(...)
        end
        return true
    ]])
    eval('router_1', [[
        require('fiber').create(function()
            local ok, result = pcall(
                require('vector_search.router').search, 'docs', {1, 0},
                {k = 1, L = 1, scope = {kind = 'bucket',
                                       bucket_id = 2001}, timeout = 5})
            rawset(_G, 'vector_ref_finished',
                   ok and #result == 1 and result[1].bucket_id == 2001)
        end)
        return true
    ]])
    local entered = false
    for _ = 1, 100 do
        local result = test_run:eval(names[ref_source],
            "return rawget(_G, 'vector_ref_entered') == true")
        if result[1] then
            entered = true
            break
        end
        fiber.sleep(0.01)
    end
    assert(entered)
    local send_started = fiber.clock()
    eval(names[ref_source],
         "return vshard.storage.bucket_send(2001, '" ..
         util.replicasets[ref_target] .. "', {timeout = 10})")
    local send_seconds = fiber.clock() - send_started
    local benchmark_output = os.getenv('VSHARD_VECTOR_BENCH_OUTPUT')
    if benchmark_output ~= nil then
        local json = require('json')
        local file = assert(io.open(benchmark_output, 'r'))
        local report = json.decode(file:read('*a'))
        file:close()
        report.migration_with_protected_search_seconds = send_seconds
        file = assert(io.open(benchmark_output, 'w'))
        file:write(json.encode(report), '\n')
        file:close()
    end
    eval('router_1',
         "return rawget(_G, 'vector_ref_finished') == true")
    eval(names[ref_source], [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', after_move)
    eval('storage_3_a', [[
        box.space.vector_docs.index.vec:alter({distance = 'ip'})
        return true
    ]])
    eval('router_1', [[
        local ok = pcall(require('vector_search.router').search,
                         'docs', {1, 0}, {k = 3, L = 3, timeout = 5})
        return not ok
    ]])
    eval('storage_3_a', [[
        box.space.vector_docs.index.vec:alter({distance = 'l2'})
        return true
    ]])
    eval('router_1', after_move)
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(...)
            local result = storage.saved_search(...)
            result.version = 2
            return result
        end
        return true
    ]])
    eval('router_1', [[
        local ok = pcall(require('vector_search.router').search,
                         'docs', {1, 0}, {k = 3, L = 3, timeout = 5})
        return not ok
    ]])
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', [[
        local router = require('vector_search.router')
        router.configure('docs', {dimension = 2, distance = 'l2',
            pk_parts = {{fieldno = 1, type = 'unsigned'}},
            max_pk_bytes = 1, max_response_bytes = 8192})
        return true
    ]])
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(...)
            local result = storage.saved_search(...)
            result.padding = string.rep('x', 20000)
            return result
        end
        return true
    ]])
    eval('router_1', [[
        local ok = pcall(require('vector_search.router').search,
                         'docs', {1, 0}, {k = 1, L = 1, timeout = 5})
        return not ok
    ]])
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', [[
        require('vector_search.router').configure('docs', {
            dimension = 2, distance = 'l2',
            pk_parts = {{fieldno = 1, type = 'unsigned'}}})
        return true
    ]])
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(...)
            require('fiber').sleep(0.3)
            local ok, result = pcall(storage.saved_search, ...)
            rawset(_G, 'vector_delay_done', true)
            if not ok then error(result) end
            return result
        end
        return true
    ]])
    eval('router_1', [[
        local ok = pcall(require('vector_search.router').search,
                         'docs', {1, 0}, {k = 1, L = 1, timeout = 0.1})
        return not ok
    ]])
    fiber.sleep(0.5)
    eval('storage_3_a',
         "return rawget(_G, 'vector_delay_done') == true")
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', after_move)
    local promote = [=[
        local util = require('util')
        local replicas = cfg.sharding[util.replicasets[1]].replicas
        replicas[util.name_to_uuid.storage_1_a].master = false
        replicas[util.name_to_uuid.storage_1_b].master = true
        vshard.storage.cfg(cfg, instance_uuid)
        return true
    ]=]
    eval('storage_1_a', promote)
    eval('storage_1_b', promote)
    eval('storage_1_b', setup)
    eval('router_1', [=[
        local util = require('util')
        local replicas = cfg.sharding[util.replicasets[1]].replicas
        replicas[util.name_to_uuid.storage_1_a].master = false
        replicas[util.name_to_uuid.storage_1_b].master = true
        vshard.router.cfg(cfg)
        return true
    ]=])
    eval('router_1', after_move)
    eval('storage_1_b', 'box.snapshot() return true')
    test_run:cmd('stop server storage_1_b')
    test_run:cmd('start server storage_1_b')
    eval('storage_1_b', promote)
    eval('storage_1_b', setup)
    eval('router_1', after_move)
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function() error('injected storage failure') end
        return true
    ]])
    eval('router_1', [[
        local ok = pcall(require('vector_search.router').search,
                         'docs', {1, 0}, {k = 3, L = 3, timeout = 5})
        return not ok
    ]])
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    eval('router_1', after_move)
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.saved_search = storage.search
        storage.search = function(...)
            rawset(_G, 'vector_disconnect_entered', true)
            require('fiber').sleep(0.3)
            local ok, result = pcall(storage.saved_search, ...)
            rawset(_G, 'vector_disconnect_done', true)
            if not ok then error(result) end
            return result
        end
        return true
    ]])
    eval('router_1', [[
        require('fiber').create(function()
            pcall(require('vector_search.router').search,
                  'docs', {1, 0}, {k = 1, L = 1, timeout = 2})
        end)
        return true
    ]])
    local disconnect_entered = false
    for _ = 1, 100 do
        local result = test_run:eval('storage_3_a',
            "return rawget(_G, 'vector_disconnect_entered') == true")
        if result[1] then
            disconnect_entered = true
            break
        end
        fiber.sleep(0.01)
    end
    assert(disconnect_entered)
    test_run:cmd('stop server router_1')
    fiber.sleep(0.5)
    eval('storage_3_a',
         "return rawget(_G, 'vector_disconnect_done') == true")
    eval('storage_3_a', [[
        local storage = require('vector_search.storage')
        storage.search = storage.saved_search
        storage.saved_search = nil
        return true
    ]])
    test_run:cmd('start server router_1')
    eval('router_1', [=[
        local util = require('util')
        local replicas = cfg.sharding[util.replicasets[1]].replicas
        replicas[util.name_to_uuid.storage_1_a].master = false
        replicas[util.name_to_uuid.storage_1_b].master = true
        vshard.router.cfg(cfg)
        require('vector_search.router').configure('docs', {
            dimension = 2, distance = 'l2',
            pk_parts = {{fieldno = 1, type = 'unsigned'}}})
        return true
    ]=])
    eval('router_1', after_move)
end

local ok, err = pcall(run)
pcall(test_run.cmd, test_run, 'stop server router_1')
pcall(test_run.cmd, test_run, 'cleanup server router_1')
pcall(test_run.drop_cluster, test_run, rs1)
pcall(test_run.drop_cluster, test_run, rs2)
pcall(test_run.drop_cluster, test_run, rs3)
pcall(test_run.cmd, test_run, 'clear filter')
if not ok then
    error(err)
end
print('vector_cluster_basic=pass')
return true
