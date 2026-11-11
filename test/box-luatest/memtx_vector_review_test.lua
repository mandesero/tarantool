local server = require('luatest.server')
local t = require('luatest')

local g = t.group()

g.before_all(function(cg)
    cg.server = server:new({box_cfg = {memtx_use_mvcc_engine = true}})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.before_each(function(cg)
    cg.server:exec(function()
        local s = box.schema.space.create('vector_review')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'array'},
                  {name = 'payload', type = 'string'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                              distance = 'l2', unique = false,
                              parts = {{2, 'array'}}})
    end)
end)

g.after_each(function(cg)
    cg.server:exec(function()
        box.error.injection.set('ERRINJ_VECTOR_REBUILD_DELAY', false)
        box.error.injection.set('ERRINJ_VECTOR_SCAN_DELAY', false)
        box.rollback()
        for _, rv in ipairs(box.read_view.list()) do
            box.read_view.open({id = rv.id}):close()
        end
        if box.space.vector_review then box.space.vector_review:drop() end
    end)
end)

g.test_return_positions = function(cg)
    cg.server:exec(function()
        local idx = box.space.vector_review.index.vec
        local rows, pos, distances = idx:search({0, 0}, {
            limit = 0, with_distance = true,
        })
        t.assert_equals({rows, distances}, {{}, {}})
        t.assert_equals(pos, nil)
        t.assert_equals(select('#', idx:search({0, 0}, {
            limit = 0, with_distance = true,
        })), 3)
    end)
end

g.test_scan_cancel_and_drop = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.space.vector_review
        s:insert{1, {1, 0}, 'row'}
        box.error.injection.set('ERRINJ_VECTOR_SCAN_DELAY', true)
        local entered = fiber.channel(1)
        local reader = fiber.new(function()
            local iter = s.index.vec:pairs({{0, 0}}, {iterator = 'neighbor'})
            entered:put(true)
            return iter:totable()
        end)
        reader:set_joinable(true)
        entered:get()
        t.helpers.retrying({}, function()
            t.assert_equals(reader:status(), 'suspended')
        end)
        s.index.vec:drop()
        box.error.injection.set('ERRINJ_VECTOR_SCAN_DELAY', false)
        local ok, row = reader:join()
        t.assert(ok)
        t.assert_equals(row, {})
        s:create_index('vec', {type = 'vector', dimension = 2,
                              distance = 'l2', unique = false,
                              parts = {{2, 'array'}}})
        box.error.injection.set('ERRINJ_VECTOR_SCAN_DELAY', true)
        reader = fiber.new(function()
            local iter = s.index.vec:pairs({{0, 0}}, {iterator = 'neighbor'})
            entered:put(true)
            return iter:head()
        end)
        reader:set_joinable(true)
        entered:get()
        t.helpers.retrying({}, function()
            t.assert_equals(reader:status(), 'suspended')
        end)
        reader:cancel()
        ok = reader:join()
        t.assert_equals(ok, false)
        box.error.injection.set('ERRINJ_VECTOR_SCAN_DELAY', false)
    end)
end

g.test_unbounded_ordered_pairs = function(cg)
    cg.server:exec(function()
        local fiber = require('fiber')
        local s = box.space.vector_review
        for i = 1, 1100 do s:insert{i, {i, 0}, 'row'} end
        local requests = s.index.vec:stat().search.requests
        local count = 0
        for _, row in s.index.vec:pairs({{0, 0}}, {iterator = 'neighbor'}) do
            count = count + 1
            t.assert_equals(row[1], count)
            if count % 31 == 0 then fiber.sleep(0) end
        end
        t.assert_equals(count, 1100)
        t.assert_equals(s.index.vec:stat().search.requests, requests)
        t.assert_equals(#s.index.vec:pairs():totable(), 1100)
        t.assert_equals(s.index.vec:count({{0, 0}},
                                       {iterator = 'neighbor'}), 1100)
        t.assert_error_covers({code = box.error.VECTOR_UNSUPPORTED},
            function()
                s.index.vec:pairs({{0, 0}},
                                 {iterator = 'neighbor', limit = 1})
            end)
    end)
end

g.test_pairs_own_changes_and_stale_batch = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_review
        for i = 1, 40 do s:insert{i, {i, 0}, 'old'} end
        box.begin()
        s:replace{1, {100, 0}, 'new'}
        local rows = s.index.vec:pairs({{0, 0}},
                                     {iterator = 'neighbor'}):totable()
        t.assert_equals(rows[1][1], 2)
        t.assert_equals(rows[#rows][1], 1)
        box.rollback()
        local gen, param, state = s.index.vec:pairs({{0, 0}},
                                                   {iterator = 'neighbor'})
        local row
        state, row = gen(param, state)
        t.assert_equals(row[1], 1)
        s:delete{2}
        local _, next_row = gen(param, state)
        t.assert_equals(next_row[1], 3)
    end)
end

g.test_read_view_survives_dml_rebuild_and_drop = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_review
        for i = 1, 70 do s:insert{i, {i, 0}, 'original'} end
        local rv = box.read_view.open()
        s:replace{1, {100, 0}, 'changed'}
        s:delete{2}
        s.index.vec:rebuild()
        s:drop()
        rv:with(function()
            local idx = rv.space.vector_review.index.vec
            local rows = idx:select({{0, 0}},
                                    {iterator = 'neighbor', limit = 70})
            t.assert_equals(#rows, 70)
            for i, row in ipairs(rows) do
                t.assert_equals(row, {i, {i, 0}, 'original'})
            end
            local count = 0
            for _ in idx:pairs() do count = count + 1 end
            t.assert_equals(count, 70)
        end)
        rv:close()
    end)
end

g.test_read_view_with_hash_primary = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_review
        s.index.vec:drop()
        s.index.pk:drop()
        s:create_index('pk', {type = 'hash'})
        s:create_index('vec', {type = 'vector', dimension = 2,
                              distance = 'l2', unique = false,
                              parts = {{2, 'array'}}})
        s:insert{1, {1, 0}, 'committed'}
        box.begin()
        s:replace{1, {2, 0}, 'pending'}
        local rv = box.read_view.open()
        box.rollback()
        rv:with(function()
            local rows = rv.space.vector_review.index.vec:select({{0, 0}},
                {iterator = 'neighbor', limit = 1})
            t.assert_equals(rows, {{1, {1, 0}, 'committed'}})
        end)
        rv:close()
    end)
end

g.test_node_bound_with_delayed_maintenance = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_review
        s:insert{1, {1, 0}, 'row'}
        box.error.injection.set('ERRINJ_VECTOR_REBUILD_DELAY', true)
        local rejected = false
        for i = 2, 300 do
            local previous = s:get{1}
            local ok, err = pcall(s.replace, s, {1, {i, 0}, 'row'})
            if not ok then
                t.assert_equals(err.type, 'OutOfMemory')
                t.assert_equals(s:get{1}, previous)
                rejected = true
                break
            end
        end
        t.assert(rejected)
        local stat = s.index.vec:stat()
        t.assert(stat.slots.nodes <= stat.slots.node_limit)
        box.error.injection.set('ERRINJ_VECTOR_REBUILD_DELAY', false)
        t.helpers.retrying({}, function()
            s:replace{1, {999, 0}, 'resumed'}
        end)
        t.assert_equals(s.index.vec:search({999, 0}, {limit = 1})[1][3],
                        'resumed')
    end)
end

g.test_packed_coordinates = function(cg)
    cg.server:exec(function()
        local s = box.space.vector_review
        s:drop()
        s = box.schema.space.create('vector_review')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'varbinary'},
                  {name = 'payload', type = 'string'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                              distance = 'cosine', unique = false,
                              parts = {{2, 'varbinary'}}})
        local packed = box.vector.pack({1, 0})
        t.assert_equals(box.vector.unpack(packed), {1, 0})
        s:insert{1, packed, 'first'}
        s:insert{2, box.vector.pack({0, 1}), 'other'}
        local rows, pos, distances = s.index.vec:search({1, 0}, {
            limit = 2, with_distance = true,
        })
        t.assert_equals(pos, nil)
        t.assert_equals({rows[1][1], distances[1]}, {1, 0})
        t.assert_equals(distances[2], 1)
        t.assert_equals(s.index.vec:stat().memory.vectors, 0)
        t.assert_equals(s.index.vec:stat().memory.borrowed_vectors, 16)
        for i = 1, 100 do s:update({1}, {{'=', 3, tostring(i)}}) end
        t.assert_equals(s.index.vec:stat().slots.embeddings, 2)
        local rv = box.read_view.open()
        s:delete{1}
        s.index.vec:rebuild()
        rv:with(function()
            local got = rv.space.vector_review.index.vec:select({{1, 0}},
                {iterator = 'neighbor', limit = 2})
            t.assert_equals(got[1][1], 1)
            t.assert_equals(got[1][2], packed)
        end)
        rv:close()
        t.assert_equals(s.index.vec:pairs({{1, 0}},
            {iterator = 'neighbor'}):head()[1], 2)
        t.assert_error(function() box.vector.pack({0/0, 1}) end)
        t.assert_error(function() box.vector.pack({x = 1}) end)
        t.assert_error(function() box.vector.pack({1, extra = 2}) end)
        local varbinary = require('varbinary')
        t.assert_error(function() s:insert{3, varbinary.new('short'), ''} end)
        t.assert_error(function()
            s:insert{3, varbinary.new(string.char(0, 0, 128, 127,
                                                0, 0, 0, 0)), ''}
        end)
        t.assert_error(function() s:insert{3, box.vector.pack({0, 0}), ''} end)
        local valid = box.vector.pack({1, 1})
        for nth = 0, 30 do
            box.error.injection.set('ERRINJ_VECTOR_ALLOC', nth)
            local ok = pcall(s.insert, s, {3, valid, 'allocation failure'})
            box.error.injection.set('ERRINJ_VECTOR_ALLOC', -1)
            if ok then s:delete{3} end
            t.assert_equals(s:get{3}, nil)
            t.assert_equals(s:get{2}[3], 'other')
        end
        t.helpers.retrying({}, function()
            s:update({2}, {{'=', 3, 'other'}})
            t.assert_equals(s.index.vec:stat().slots.embeddings, 1)
        end)
        s.index.vec:rebuild()
        t.assert_equals(s.index.vec:stat().memory.vectors, 0)
        t.assert_equals(s.index.vec:stat().memory.borrowed_vectors, 8)
    end)
end
