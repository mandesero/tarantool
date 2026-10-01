local server = require('luatest.server')
local t = require('luatest')

local g = t.group('vector_replication')

g.before_all(function(cg)
    cg.master = server:new({alias = 'vector_master'})
    cg.replica = server:new({
        alias = 'vector_replica',
        box_cfg = {
            read_only = true,
            replication = cg.master.net_box_uri,
        },
    })
    cg.master:start()
end)

g.after_all(function(cg)
    cg.replica:drop()
    cg.master:drop()
end)

g.test_snapshot_join_and_async_apply = function(cg)
    cg.master:exec(function()
        local s = box.schema.space.create('vector_repl')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'array'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                               unique = false, parts = {{2, 'array'}}})
        s:insert{1, {1, 0}}
        s:insert{2, {0, 1}}
        box.snapshot()
    end)
    cg.replica:start()
    cg.replica:wait_for_vclock_of(cg.master)
    cg.replica:exec(function()
        local s = box.space.vector_repl
        t.assert_equals(s.index.vec:len(), 2)
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 2, with_distance = true,
        })
        t.assert_equals({rows[1].tuple[1], rows[1].distance}, {1, 0})
    end)
    cg.master:exec(function()
        local s = box.space.vector_repl
        s:replace{2, {0.9, 0.1}}
        s:delete{1}
        s:insert{3, {1, 0}}
    end)
    cg.replica:wait_for_vclock_of(cg.master)
    cg.replica:exec(function()
        local s = box.space.vector_repl
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 3, with_distance = true,
        })
        t.assert_equals(#rows, 2)
        t.assert_equals({rows[1].tuple[1], rows[1].distance}, {3, 0})
        t.assert_equals(rows[2].tuple[1], 2)
    end)
end
