local t = require('luatest')
local justrun = require('luatest.justrun')

local g = t.group('gh-4975')

--[[
Require a non-existent module with a long enough name to get an error message,
which is longer than 1000 characters, like:

LuajitError: module 'aaa<...>aaa' not found:
    no file '/usr/local/lib/lua/5.1/aaa<...>aaa.so'
    no file '/usr/local/lib/lua/5.1/override/aaa<...>aaa.so'
    <...>
]]
g.test_long_init_error = function()
    local module_name = string.rep('a', 500)
    local args = {'-l', module_name, '-e', '""'}
    local ref = ("module '%s' not found:.*"):format(module_name)
    -- The child exits during startup, so stderr collection via justrun may
    -- occasionally miss part of the fatal error. Retry until the complete
    -- "module ... not found" report is observed and verify it is not stripped.
    t.helpers.retrying({timeout = 5}, function()
        local res = justrun.tarantool('.', {}, args,
                                      {stderr = true, nojson = true})
        t.assert_str_matches(res.stderr, ref)
        t.assert_gt(string.len(res.stderr), 1000,
                    'error is longer than 1000 chars')
    end)
end
