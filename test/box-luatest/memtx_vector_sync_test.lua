local server = require('luatest.server')
local t = require('luatest')

local g = t.group('memtx_vector_sync')

g.before_all(function(cg)
    cg.server = server:new({box_cfg = {
        replication_synchro_quorum = 1,
        replication_synchro_timeout = 0.05,
    }})
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_timeout_rollback_preserves_vector_index = function(cg)
    cg.server:exec(function()
        local s = box.schema.space.create('vector_sync', {is_sync = true})
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'vec', type = 'array'}})
        s:create_index('pk')
        s:create_index('vec', {type = 'vector', dimension = 2,
                               unique = false, parts = {{2, 'array'}}})
        box.ctl.promote()
        s:insert{1, {1, 0}}
        box.cfg{replication_synchro_quorum = 2}
        local ok = pcall(function() s:replace{1, {0, 1}} end)
        t.assert_equals(ok, false)
        t.helpers.retrying({}, function()
            t.assert_equals(box.info.synchro.queue.len, 0)
        end)
        local rows = s.index.vec:select({{1, 0}}, {
            iterator = 'neighbor', limit = 1, with_distance = true,
        })
        t.assert_equals({rows[1].tuple[2], rows[1].distance},
                        {{1, 0}, 0})
    end)
end
