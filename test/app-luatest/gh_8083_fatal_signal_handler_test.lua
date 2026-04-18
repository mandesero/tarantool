local fio = require('fio')
local fiber = require('fiber')
local t = require('luatest')

local g = t.group('gh-8083', {{errinj = 'ERRINJ_SIGILL_MAIN_THREAD'},
                              {errinj = 'ERRINJ_SIGILL_NONMAIN_THREAD'}})

g.before_each(function(cg)
    cg.tempdir = fio.tempdir()
end)

g.after_each(function(cg)
    fio.rmtree(cg.tempdir)
end)

-- Check that forked Tarantool creates a crash report on the illegal instruction
g.test_fatal_signal_handler = function(cg)
    local popen = require('popen')
    t.tarantool.skip_if_not_debug()

    local tarantool_exe = arg[-1]
    local tarantool_env = os.environ()
    tarantool_env[cg.params.errinj] = 'true'

    -- Use `cd' and `shell = true' due to lack of cwd option in popen (gh-5633),
    -- `feedback_enabled = false' to avoid forking for sending feedback.
    local fmt = 'cd %s && %s -e "box.cfg{feedback_enabled = false} os.exit()"'
    local cmd = string.format(fmt, cg.tempdir, tarantool_exe)
    local ph = popen.new({cmd}, {stderr = popen.opts.PIPE, env = tarantool_env,
                                 shell = true})
    t.assert(ph)
    local chunks = {}
    local stderr_fiber = fiber.new(function()
        while true do
            local chunk, err = ph:read({stderr = true})
            if chunk == nil then
                return nil, err
            end
            if chunk == '' then
                return table.concat(chunks)
            end
            table.insert(chunks, chunk)
        end
    end)
    stderr_fiber:set_joinable(true)

    -- Keep draining stderr while waiting for the crash report. Otherwise the
    -- report may fill the pipe and block the child in write(), so ph:wait()
    -- never returns.
    ph:wait()
    local ok, output_or_err = stderr_fiber:join()
    t.assert(ok, output_or_err)
    t.assert_str_contains(output_or_err, "Please file a bug")
    ph:close()
end
