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
        local s = box.schema.space.create('vector_lifecycle')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'array'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                               distance = 'l2', unique = false,
                               opts = {ef_search = 32},
                               parts = {{2, 'array'}}})
    end)
end)

g.after_each(function(cg)
    cg.server:exec(function()
        if box.space.vector_lifecycle ~= nil then
            box.space.vector_lifecycle:drop()
        end
    end)
end)

g.test_search_width_alter_avoids_rebuild = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        local before = s.index.vec:bsize()
        local generation = s.index.vec:stat().hnsw.generation
        s.index.vec:alter({opts = {ef_search = 128}})
        t.assert_equals(s.index.vec:bsize(), before)
        t.assert_equals(s.index.vec:stat().hnsw.generation, generation)
        t.assert_equals(s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 1)
        t.assert_equals(box.space._index:get{s.id, 1}[5].opts.ef_search,
                        128)
    end)
end

g.test_statistics_and_read_view_rejection = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        s:insert{2, {0, 1}}
        s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 1,
            filter = {field = 'id', values = {2}},
        })
        local stat = s.index.vec:stat()
        t.assert_equals(stat.config.dimension, 2)
        t.assert_equals(stat.config.distance, 'l2')
        t.assert_equals(stat.config.scalar, 'float32')
        t.assert_equals(stat.config.numeric_contract, 'f32_f64_v1')
        t.assert_equals(stat.limits, {
            dimension = 4096, limit = 1024, ef_search = 8192,
            filter_values = 65536, timeout = 30, default_timeout = 1,
            work = 2^32,
        })
        t.assert_equals(stat.versions.live, 2)
        t.assert(stat.slots.capacity >= 2)
        t.assert_equals(stat.search.requests, 1)
        t.assert(stat.search.filtered_candidates >= 1)
        t.assert(stat.search.duration_ns > 0)
        local duration = stat.search.duration_ns
        s.index.vec:select({{1, 0}}, {iterator = 'neighbor', limit = 0})
        t.assert(s.index.vec:stat().search.duration_ns > duration)
        duration = s.index.vec:stat().search.duration_ns
        local ok = pcall(function()
            s.index.vec:select({{0/0, 0}}, {
                iterator = 'neighbor', limit = 1,
            })
        end)
        t.assert_equals(ok, false)
        stat = s.index.vec:stat()
        t.assert_equals(stat.search.requests, 3)
        t.assert_equals(stat.search.errors, 1)
        t.assert(stat.search.duration_ns > duration)
        t.assert_equals(stat.memory.total, s.index.vec:bsize())
        local categories = stat.memory.graph + stat.memory.vectors +
                           stat.memory.lookup + stat.memory.retained
        t.assert_equals(categories, stat.memory.total)
        local ok, err = pcall(box.read_view.open)
        t.assert_equals(ok, false)
        t.assert_equals(err.code, box.error.VECTOR_UNSUPPORTED)
    end)
end

g.test_retired_slots_wait_for_rebuild = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        s:delete{1}
        local stat = s.index.vec:stat()
        t.assert_equals(stat.slots.pending_rebuild, 1)
        s:insert{2, {0, 1}}
        t.assert_equals(s.index.vec:stat().slots.pending_rebuild, 1)
        box.begin()
        s:replace{2, {2, 0}}
        box.rollback()
        t.assert_equals(s.index.vec:stat().slots.pending_rebuild, 2)
        t.assert_equals(s:get{2}[2], {0, 1})
        s.index.vec:rebuild()
        stat = s.index.vec:stat()
        t.assert_equals(stat.slots.pending_rebuild, 0)
        t.assert_equals(stat.versions.retired, 0)
        t.assert_equals(stat.versions.live, 1)
        t.assert_equals(s.index.vec:select({{0, 1}}, {
            iterator = 'neighbor', limit = 1,
        })[1][2], {0, 1})
    end)
end

g.test_failed_metric_alter_keeps_old_index = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        s:insert{2, {0, 1}}
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', 0)
        local ok = pcall(function()
            s.index.vec:alter({distance = 'ip'})
        end)
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
        t.assert_equals(ok, false)
        t.assert_equals(box.space._index:get{s.id, 1}[5].distance, 'l2')
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 2, with_distance = true,
        })
        t.assert_equals({rows[1].tuple[1], rows[1].distance}, {1, 0})
    end)
end

g.test_snapshot_then_wal_updates = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        s:insert{2, {0, 1}}
        box.snapshot()
        s:replace{2, {0.9, 0.1}}
        s:delete{1}
        s:insert{3, {1, 0}}
    end)
    cg.server:restart()
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 3, with_distance = true,
        })
        t.assert_equals(#rows, 2)
        t.assert_equals({rows[1].tuple[1], rows[1].distance}, {3, 0})
        t.assert_equals(rows[2].tuple[1], 2)
    end)
end

g.test_large_snapshot_recovers_beyond_client_slice = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.schema.space.create('vector_recovery_large')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'array'}})
        s:create_index('pk')
        for id = 1, 1000 do
            local vector = {}
            for j = 1, 16 do
                vector[j] = ((id * 37 + j * 19) % 1000) / 500 - 1
            end
            s:insert{id, vector}
        end
        fiber.set_slice(120)
        s:create_index('vec', {type = 'vector', dimension = 16,
                               distance = 'l2', unique = false,
                               parts = {{2, 'array'}}})
        fiber.set_slice(1)
        box.snapshot()
    end)
    cg.server:restart()
    cg.server:exec(function()
        local s = box.space.vector_recovery_large
        t.assert_equals(s:len(), 1000)
        t.assert_equals(s.index.vec:stat().versions.live, 1000)
        s:drop()
    end)
end

g.test_rebuild_failure_and_generation_switch = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        for i = 1, 20 do
            s:insert{i, {i, 0}}
        end
        local generation = s.index.vec:stat().hnsw.generation
        local before = s.index.vec:bsize()
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', 0)
        local ok = pcall(function() s.index.vec:rebuild() end)
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
        t.assert_equals(ok, false)
        t.assert_equals(s.index.vec:stat().hnsw.generation, generation)
        t.assert_equals(s.index.vec:bsize(), before)
        t.assert_equals(s.index.vec:select({{20, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 20)
        s.index.vec:rebuild()
        t.assert_not_equals(s.index.vec:stat().hnsw.generation,
                            generation)
        t.assert_equals(s.index.vec:len(), 20)
        t.assert_equals(s.index.vec:select({{20, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 20)
    end)
end

g.test_rebuild_rejects_active_transaction = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        box.begin()
        local ok, err = pcall(function() s.index.vec:rebuild() end)
        box.rollback()
        t.assert_equals(ok, false)
        t.assert_equals(err.code, box.error.ACTIVE_TRANSACTION)
    end)
end

g.test_create_on_nonempty_and_failed_create = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s.index.vec:drop()
        for i = 1, 8 do
            s:insert{i, {i, 0}}
        end
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', 0)
        local ok = pcall(function()
            s:create_index('vec', {type = 'vector', dimension = 2,
                                   distance = 'l2', unique = false,
                                   parts = {{2, 'array'}}})
        end)
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
        t.assert_equals(ok, false)
        t.assert_equals(s.index.vec, nil)
        t.assert_equals(s:count(), 8)
        s:create_index('vec', {type = 'vector', dimension = 2,
                               distance = 'l2', unique = false,
                               parts = {{2, 'array'}}})
        t.assert_equals(s.index.vec:len(), 8)
        t.assert_equals(s.index.vec:select({{8, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 8)
        s:truncate()
        t.assert_equals(s.index.vec:len(), 0)
        t.assert_equals(s.index.vec:select({{8, 0}}, {
            iterator = 'neighbor', limit = 1,
        }), {})
    end)
end

g.test_structural_alter_rebuilds = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        local generation = s.index.vec:stat().hnsw.generation
        s.index.vec:alter({distance = 'ip'})
        t.assert_not_equals(s.index.vec:stat().hnsw.generation,
                            generation)
        t.assert_equals(s.index.vec:stat().config.distance, 'ip')
        t.assert_equals(s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 1, with_distance = true,
        })[1].distance, 0)
        local after = s.index.vec:stat().hnsw.generation
        local ok = pcall(function()
            s.index.vec:alter({dimension = 3})
        end)
        t.assert_equals(ok, false)
        t.assert_equals(s.index.vec:stat().hnsw.generation, after)
        t.assert_equals(s.index.vec:stat().config.dimension, 2)
    end)
end

g.test_rebuild_midway_oom_and_slice = function(cg)
    t.tarantool.skip_if_not_debug()
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.space.vector_lifecycle
        for i = 1, 20 do
            s:insert{i, {i, 0}}
        end
        local failures = 0
        for countdown = 0, 48 do
            local generation = s.index.vec:stat().hnsw.generation
            box.error.injection.set('ERRINJ_VECTOR_ALLOC', countdown)
            local ok = pcall(function() s.index.vec:rebuild() end)
            box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
            if not ok then
                failures = failures + 1
                t.assert_equals(s.index.vec:stat().hnsw.generation,
                                generation)
            end
            t.assert_equals(s.index.vec:select({{20, 0}}, {
                iterator = 'neighbor', limit = 1,
            })[1][1], 20)
        end
        t.assert_gt(failures, 10)
        local generation = s.index.vec:stat().hnsw.generation
        fiber.set_slice(0)
        local ok = pcall(function() s.index.vec:rebuild() end)
        fiber.set_slice(1)
        t.assert_equals(ok, false)
        t.assert_equals(s.index.vec:stat().hnsw.generation, generation)
    end)
end

g.test_dml_during_yielding_index_build = function(cg)
    t.tarantool.skip_if_not_debug()
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.space.vector_lifecycle
        s.index.vec:drop()
        for i = 1, 20 do
            s:insert{i, {i, 0}}
        end
        box.error.injection.set('ERRINJ_BUILD_INDEX_DELAY', true)
        local builder = fiber.new(function()
            s:create_index('vec', {type = 'vector', dimension = 2,
                                   distance = 'l2', unique = false,
                                   parts = {{2, 'array'}}})
        end)
        builder:set_joinable(true)
        fiber.yield()
        s:replace{20, {200, 0}}
        s:delete{1}
        s:insert{21, {21, 0}}
        box.error.injection.set('ERRINJ_BUILD_INDEX_DELAY', false)
        local ok, err = builder:join()
        t.assert(ok, err)
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 32,
        })
        local ids = {}
        for _, row in ipairs(rows) do
            table.insert(ids, row[1])
        end
        table.sort(ids)
        local expected = {}
        for _, row in ipairs(s.index.pk:select()) do
            table.insert(expected, row[1])
        end
        t.assert_equals(ids, expected)
        t.assert_equals(s.index.vec:len(), s:count())
        t.assert_equals(s.index.vec:select({{200, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 20)
    end)
end

g.test_failed_truncate_preserves_index = function(cg)
    t.tarantool.skip_if_not_debug()
    cg.server:exec(function()
        local s = box.space.vector_lifecycle
        s:insert{1, {1, 0}}
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', 0)
        local ok = pcall(function() s:truncate() end)
        box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
        t.assert_equals(ok, false)
        t.assert_equals(s:get{1}[2], {1, 0})
        t.assert_equals(s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 1,
        })[1][1], 1)
    end)
end
