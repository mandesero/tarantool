local fio = require('fio')
local fiber = require('fiber')
local t = require('luatest')
local g = t.group('gh-8445')

g.before_each(function(cg)
    cg.tempdir = fio.tempdir()
end)

g.after_each(function(cg)
    fio.rmtree(cg.tempdir)
end)

-- Check that forked Tarantool doesn't crash when preparing a crash report.
g.test_crash_during_crash_report = function(cg)
    local ffi = require('ffi')
    local popen = require('popen')

    -- Use `cd' and `shell = true' due to lack of cwd option in popen (gh-5633).
    local exe = arg[-1]
    local dir = cg.tempdir
    local script = [[
        local tweaks = require('internal.tweaks')
        local log = require('log')
        box.cfg{}
        tweaks.crash_produce_coredump = false
        log.info('pid = ' .. box.info.pid)
    ]]
    local ph = popen.new({string.format('cd %s && %s -e "%s"',
                                         dir, exe, script)},
                         {stdout = popen.opts.DEVNULL,
                          stderr = popen.opts.PIPE, shell = true})
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

    local output = ''
    -- Keep draining stderr while waiting for box.cfg{} completion. Otherwise
    -- the crash report may fill the pipe and block the child in write() while
    -- the parent is already waiting in ph:wait().
    t.helpers.retrying({timeout = 5}, function()
        output = table.concat(chunks)
        t.assert_str_contains(output, "pid = ")
    end)

    -- ph:info().pid won't work, because it returns pid of the shell rather than
    -- pid of the Tarantool.
    local pid = tonumber(string.match(output, "pid = (%d+)"))
    t.assert(pid)
    ffi.C.kill(pid, popen.signal.SIGILL)
    ph:wait()
    local ok, output_or_err = stderr_fiber:join()
    t.assert(ok, output_or_err)
    output = output_or_err
    t.assert_str_contains(output, "Please file a bug")

    -- Check that there were no fatal signals during crash report.
    t.assert_not_str_contains(output, "Fatal 11 while backtracing")
    ph:close()
end
