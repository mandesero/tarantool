require('test.metrics-luatest.helper')

local t = require('luatest')
local g = t.group('box-cfg-metrics')
local server = require('luatest.server')

local utils = require('third_party.metrics.test.utils')

g.before_all(function()
    g.server = server:new()
    g.server:start()
end)

g.after_all(function()
    g.server:drop()
end)

g.before_each(function()
    g.server:exec(function()
        box.cfg{
            metrics = {
                include = 'all',
                exclude = {},
                labels = {},
            }
        }
    end)
end)

g.test_include = function()
    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                include = {'info'},
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    local uptime = utils.find_metric('tnt_info_uptime', default_metrics)
    t.assert_not_equals(uptime, nil)
    local memlua = utils.find_metric('tnt_info_memory_lua', default_metrics)
    t.assert_equals(memlua, nil)
end

g.test_exclude = function()
    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                exclude = {'memory'},
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    local uptime = utils.find_metric('tnt_info_uptime', default_metrics)
    t.assert_not_equals(uptime, nil)
    local memlua = utils.find_metric('tnt_info_memory_lua', default_metrics)
    t.assert_equals(memlua, nil)
end

g.test_include_with_exclude = function()
    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                include = {'info', 'memory'},
                exclude = {'memory'},
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    local uptime = utils.find_metric('tnt_info_uptime', default_metrics)
    t.assert_not_equals(uptime, nil)
    local memlua = utils.find_metric('tnt_info_memory_lua', default_metrics)
    t.assert_equals(memlua, nil)
end

g.test_include_none = function()
    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                include = 'none',
                exclude = {'memory'},
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    t.assert_equals(default_metrics, {})
end

g.test_labels = function()
    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                labels = {mylabel = 'myvalue'}
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    local uptime = utils.find_obs('tnt_info_uptime', {mylabel = 'myvalue'},
                                  default_metrics)
    t.assert_equals(uptime.label_pairs, {mylabel = 'myvalue'})

    default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                labels = {}
            }
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    uptime = utils.find_obs('tnt_info_uptime', {}, default_metrics)
    t.assert_equals(uptime.label_pairs, {})
end

local function assert_cpu_extended_presents(metrics)
    local cpu_thread = utils.find_metric('tnt_cpu_thread', metrics)
    t.assert_not_equals(cpu_thread, nil)

    local cpu_number = utils.find_metric('tnt_cpu_number', metrics)
    t.assert_not_equals(cpu_number, nil)

    local cpu_time = utils.find_metric('tnt_cpu_time', metrics)
    t.assert_not_equals(cpu_time, nil)
end

g.test_include_cpu_extended = function()
    t.skip_if(jit.os ~= 'Linux', "Linux-specific")

    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                include = {'cpu_extended'},
            },
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    assert_cpu_extended_presents(default_metrics)

    -- Assert non-extended cpu metrics are not enabled.
    local cpu_user_time = utils.find_metric('tnt_cpu_user_time',
                                            default_metrics)
    t.assert_equals(cpu_user_time, nil)
    local cpu_system_time = utils.find_metric('tnt_cpu_system_time',
                                              default_metrics)
    t.assert_equals(cpu_system_time, nil)
end

g.test_include_all_has_cpu_extended = function()
    t.skip_if(jit.os ~= 'Linux', "Linux-specific")

    local default_metrics = g.server:exec(function()
        box.cfg{
            metrics = {
                include = 'all',
            },
        }

        return require('metrics').collect{invoke_callbacks = true}
    end)

    assert_cpu_extended_presents(default_metrics)
end
