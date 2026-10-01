local server = require('luatest.server')
local t = require('luatest')

local g = t.group('vector_storage')

g.before_all(function(cg)
    cg.server = server:new()
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_storage_envelope_and_scope = function(cg)
    cg.server:exec(function()
        local context = {mode = 'map', allowed = {[1] = true,
                                                 [2] = true}}
        function context:contains(id)
            if self.mode == 'bucket' then
                return id == self.bucket_id
            end
            return self.allowed[id] == true
        end
        function context:bucket_ids(max_count)
            t.assert(max_count >= 2)
            return {1, 2}
        end
        package.loaded['vshard.storage'] = {
            call_context = function() return context end,
        }
        local s = box.schema.space.create('vector_storage')
        s:format({{name = 'id', type = 'unsigned'},
                  {name = 'bucket_id', type = 'unsigned'},
                  {name = 'embedding', type = 'array'}})
        s:create_index('pk')
        s:create_index('bucket', {parts = {{2, 'unsigned'}},
                                  unique = false})
        s:create_index('vec', {type = 'vector', dimension = 2,
                               distance = 'l2', unique = false,
                               parts = {{3, 'array'}}})
        s:insert{1, 1, {1, 0}}
        s:insert{2, 2, {0.9, 0.1}}
        s:insert{3, 3, {1, 0}}
        local storage = require('vector_search.storage')
        local config = {
            space = 'vector_storage', index = 'vec', pk_index = 'pk',
            bucket_field = 'bucket_id', bucket_index = 'bucket',
        }
        t.assert_equals(pcall(storage.configure, 'denied', config), false)
        box.session.su('admin', storage.configure, 'test', config)
        local request = {name = 'test', query = {1, 0}, L = 3,
                         scope = {kind = 'all'}}
        local function assert_error(fragment, fn)
            local ok, err = pcall(fn)
            t.assert_equals(ok, false)
            t.assert_str_contains(tostring(err), fragment)
        end
        local result = require('net.box').self:call(
            'vector_search.storage.search', {request, 1})
        t.assert_equals(result.version, 1)
        t.assert_equals(result.dimension, 2)
        t.assert_equals(result.distance, 'l2')
        t.assert_equals(result.numeric_contract, 'f32_f64_v1')
        t.assert_equals(result.covered_bucket_ids, {1, 2})
        t.assert_equals(#result.records, 2)
        t.assert_equals({result.records[1].id[1],
                         result.records[1].bucket_id,
                         result.records[1].vector,
                         result.records[1].distance},
                        {1, 1, {1, 0}, 0})
        box.session.su('admin', box.schema.user.create, 'vector_reader')
        local function call_as_reader()
            return box.session.su('vector_reader', function()
                return require('net.box').self:call(
                    'vector_search.storage.search', {request, 1})
            end)
        end
        box.session.su('admin', box.schema.user.grant, 'vector_reader',
                       'read', 'space', 'vector_storage')
        assert_error('access', call_as_reader)
        box.session.su('admin', box.schema.user.grant, 'vector_reader',
                       'execute', 'function', 'vector_search.storage.search')
        t.assert_equals(#call_as_reader().records, 2)
        box.session.su('admin', box.schema.user.revoke, 'vector_reader',
                       'read', 'space', 'vector_storage')
        assert_error('Space', call_as_reader)
        box.session.su('admin', box.schema.user.grant, 'vector_reader',
                       'read', 'space', 'vector_storage')
        request.scope = {kind = 'buckets', bucket_ids = {2, 3}}
        result = storage.search(request, 1)
        t.assert_equals(#result.records, 1)
        t.assert_equals(result.records[1].bucket_id, 2)
        request.L = 0
        t.assert_equals(#storage.search(request, 1).records, 0)
        request.L = 3
        request.scope = {kind = 'buckets', bucket_ids = {}}
        result = storage.search(request, 1)
        t.assert_equals(#result.records, 0)
        t.assert_equals(result.covered_bucket_ids, {})
        local encoded = require('msgpack').encode(result.records)
        t.assert_equals(encoded:byte(1), 0x90)
        local old_bucket_ids = context.bucket_ids
        context.bucket_ids = function() return {} end
        request.scope = {kind = 'all'}
        result = storage.search(request, 1)
        encoded = require('msgpack').encode(result.covered_bucket_ids)
        t.assert_equals(encoded:byte(1), 0x90)
        context.bucket_ids = old_bucket_ids
        request.scope = {kind = 'buckets', bucket_ids = {}}
        request.scope.bucket_ids = {[1] = 1, [3] = 2}
        assert_error('Invalid bucket set', function()
            storage.search(request, 1)
        end)
        request.scope = {kind = 'bucket', bucket_id = 3}
        result = storage.search(request, 1)
        t.assert_equals(#result.records, 0)
        request.scope = {kind = 'bucket', bucket_id = 1}
        context.mode = 'bucket'
        context.bucket_id = 1
        t.assert_equals(storage.search(request, 1).records[1].id[1], 1)
        request.scope.bucket_id = 2
        assert_error('Bucket does not match', function()
            require('net.box').self:call(
                'vector_search.storage.search', {request, 1})
        end)
        context.mode = 'map'
        local no_context = package.loaded['vshard.storage']
        no_context.call_context = function() return nil end
        assert_error('Protected vshard call', function()
            storage.search(request, 1)
        end)
        no_context.call_context = function() return context end
        request.scope = {kind = 'all'}
        assert_error('No remaining storage budget', function()
            storage.search(request, 0)
        end)
        function context:bucket_ids(max_count)
            require('fiber').sleep(0.02)
            return old_bucket_ids(self, max_count)
        end
        assert_error('Storage search deadline', function()
            storage.search(request, 0.01)
        end)
        context.bucket_ids = old_bucket_ids
        request.query = {math.huge, 0}
        assert_error('VECTOR', function()
            require('net.box').self:call(
                'vector_search.storage.search', {request, 1})
        end)
        request.query = {1, 0}
        local auth_config = table.copy(config)
        auth_config.authorize = function() return false end
        box.session.su('admin', storage.configure, 'auth', auth_config)
        request.name = 'auth'
        assert_error('not authorized', function()
            storage.search(request, 1)
        end)
        request.name = 'test'
        box.session.su('admin', storage.configure, 'small', {
            space = 'vector_storage', index = 'vec', pk_index = 'pk',
            bucket_field = 'bucket_id', bucket_index = 'bucket',
            max_response_bytes = 32,
        })
        request.name = 'small'
        request.scope = {kind = 'all'}
        assert_error('Storage response size limit', function()
            storage.search(request, 1)
        end)
        request.name = 'test'
        s.index.vec:drop()
        assert_error('Configured indexes have changed', function()
            storage.search(request, 1)
        end)
        s:drop()
        box.session.su('admin', box.schema.user.drop, 'vector_reader')
    end)
end
