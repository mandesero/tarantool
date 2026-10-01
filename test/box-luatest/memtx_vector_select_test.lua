local server = require('luatest.server')
local t = require('luatest')

local g = t.group()

g.before_all(function(cg)
    cg.server = server:new()
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.before_each(function(cg)
    cg.server:exec(function()
        local s = box.schema.space.create('vector_select')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vector', type = 'array'},
                  {name = 'bucket_id', type = 'unsigned'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                               distance = 'l2',
                               unique = false, parts = {{2, 'array'}}})
    end)
end)

g.after_each(function(cg)
    cg.server:exec(function()
        box.space.vector_select:drop()
    end)
end)

g.test_limits_and_distance = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_select
        for i = 1, 1050 do
            s:insert{i, {i / 10000, 0}, i % 2}
        end
        local query = {{0, 0}}
        for _, limit in ipairs({0, 1, 32, 33, 100, 1024}) do
            local rows = s.index.vec:select(query, {
                iterator = 'neighbor', limit = limit,
                with_distance = true,
            })
            t.assert_equals(#rows, limit)
            for j, row in ipairs(rows) do
                t.assert(box.tuple.is(row.tuple))
                t.assert_equals(row.tuple[1], j)
                t.assert(row.distance >= 0)
                if j > 1 then
                    t.assert(rows[j - 1].distance <= row.distance)
                end
            end
        end
        local tuples = s.index.vec:select(query, {
            iterator = 'neighbor', limit = 33,
        })
        t.assert(box.tuple.is(tuples[1]))
        t.assert_equals(#tuples, 33)
    end)
end

g.test_filter_before_limit = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_select
        for i = 1, 80 do
            s:insert{i, {i / 1000, 0}, i % 2}
        end
        local rows = s.index.vec:select({{0, 0}}, {
            iterator = 'neighbor', limit = 32,
            filter = {field = 'bucket_id', values = {1}},
        })
        t.assert_equals(#rows, 32)
        for i, row in ipairs(rows) do
            t.assert_equals(row[3], 1)
            t.assert_equals(row[1], i * 2 - 1)
        end
        t.assert_equals(#s.index.vec:select({{0, 0}}, {
            iterator = 'neighbor', limit = 10,
            filter = {field = 'bucket_id', values = {}},
        }), 0)
    end)
end

g.test_invalid_options = function(cg)
    cg.server:exec(function()
        local idx = box.space.vector_select.index.vec
        local key = {{0, 0}}
        local bad = {
            {iterator = 'neighbor'},
            {iterator = 'EQ'},
            {iterator = 'neighbor', limit = -1},
            {iterator = 'neighbor', limit = 1025},
            {iterator = 'neighbor', limit = 1.5},
            {iterator = 'neighbor', limit = 1, with_distance = 'yes'},
            {iterator = 'neighbor', limit = 1, timeout = 31},
            {iterator = 'neighbor', limit = 1, timeout = false},
            {iterator = 'neighbor', limit = 1, unknown = true},
            {iterator = 'neighbor', limit = 1, opts = {foo = 1}},
            {iterator = 'neighbor', limit = 1,
             opts = {ef_search = 8193}},
            {iterator = 'neighbor', limit = 1,
             filter = {field = 'vector', values = {1}}},
            {iterator = 'neighbor', limit = 1,
             filter = {field = 'bucket_id', values = {-1}}},
        }
        for _, opts in ipairs(bad) do
            t.assert_equals(pcall(function()
                idx:select(key, opts)
            end), false)
        end
    end)
end

g.test_equal_distance_and_unsigned_filter = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_select
        local value = tonumber64('18446744073709551615')
        local large = tonumber64('18446744073709551613')
        local smaller = tonumber64('18446744073709551611')
        s:insert{large, {1, 0}, value}
        s:insert{smaller, {1, 0}, value}
        s:insert{2, {1, 0}, 1}
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 3, with_distance = true,
            filter = {field = 'bucket_id', values = {value}},
        })
        t.assert_equals(#rows, 2)
        t.assert_equals(rows[1].tuple[1], smaller)
        t.assert_equals(rows[2].tuple[1], large)
        t.assert_equals(rows[1].distance, 0)
        t.assert_equals(rows[2].distance, 0)
    end)
end

g.test_composite_primary_key_tiebreak = function(cg)
    cg.server:exec(function()
        local s = box.schema.space.create('vec_composite')
        s:format({{name = 'a', type = 'unsigned'},
                  {name = 'b', type = 'string'},
                  {name = 'vec', type = 'array'}})
        s:create_index('pk', {parts = {{1, 'unsigned'},
                                         {2, 'string'}}})
        s:create_index('vec', {type = 'vector', dimension = 2,
                               distance = 'l2', unique = false,
                               parts = {{3, 'array'}}})
        s:insert{1, 'b', {1, 0}}
        s:insert{0, 'z', {1, 0}}
        s:insert{1, 'a', {1, 0}}
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 3,
        })
        t.assert_equals({rows[1][1], rows[1][2]}, {0, 'z'})
        t.assert_equals({rows[2][1], rows[2][2]}, {1, 'a'})
        t.assert_equals({rows[3][1], rows[3][2]}, {1, 'b'})
        s:drop()
    end)
end

g.test_timeout_and_generic_iterator_rejection = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_select
        s:insert{1, {1, 0}, 1}
        t.assert_equals(pcall(function()
            s.index.vec:select({{1, 0}}, {
                iterator = 'neighbor', limit = 1, timeout = 1e-12,
            })
        end), false)
        t.assert_equals(pcall(function()
            return s.index.vec:pairs({{1, 0}}, {iterator = 'neighbor'})()
        end), false)
    end)
end

g.test_heartbeat_during_selective_timeout = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        local clock = require('clock')
        local s = box.space.vector_select
        for i = 1, 2500 do
            s:insert{i, {i / 2500, (2500 - i) / 2500}, i % 20}
        end
        local running = true
        local max_lag = 0
        local interval = 0.001
        local heartbeat = fiber.new(function()
            local previous = clock.monotonic()
            while running do
                fiber.sleep(interval)
                local now = clock.monotonic()
                max_lag = math.max(max_lag, now - previous)
                previous = now
            end
        end)
        heartbeat:set_joinable(true)
        fiber.sleep(0.005)
        local ok, result = pcall(function()
            return s.index.vec:select({{1, 0}}, {
                iterator = 'neighbor', limit = 1024,
                timeout = 0.0001,
                filter = {field = 'bucket_id', values = {19}},
            })
        end)
        running = false
        heartbeat:join()
        t.assert_equals(ok, false)
        t.assert(result ~= nil)
        -- A 1 ms heartbeat may be delayed, but timeout must stop within
        -- 250 ms on this 2500-row test graph, including result handling.
        t.assert(max_lag < 0.25)
    end)
end
