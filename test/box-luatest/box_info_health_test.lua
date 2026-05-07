local t = require('luatest')
local cbuilder = require('luatest.cbuilder')
local cluster = require('luatest.cluster')
local server = require('luatest.server')

local g = t.group('box_info_health')

g.before_all(function(cg)
    cg.server = server:new()
    cg.server:start()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_default_health = function(cg)
    cg.server:exec(function()
        local health = box.info.health

        t.assert_equals(health.liveness, {
            verdict = 'alive',
            checks = {},
        })
        t.assert_equals(health.readiness, {
            status = 'ready',
            checks = {
                box = {
                    status = 'ready',
                },
            },
        })
        t.assert_equals(box.info().health, health)
    end)
end

g.test_health_with_config = function()
    local config = cbuilder:new()
        :use_group('g-001')
        :use_replicaset('r-001')
        :add_instance('i-001', {})
        :config()
    local cluster_obj = cluster:new(config)
    cluster_obj:start()

    cluster_obj['i-001']:exec(function()
        t.assert_equals(box.info.health.readiness.status, 'ready')
        t.assert_equals(box.info.health.readiness.checks.config, {
            status = 'ready',
        })
        t.assert_equals(box.info.health.readiness.checks.box, {
            status = 'ready',
        })

        local health = require('internal.healthcheck')
        t.assert_equals(health.add_health_check('app', function()
            return false, 'warming up'
        end, {alert_code = 'app.warming_up'}), true)
        t.assert_equals(box.info.health.readiness.checks.app, {
            status = 'not_ready',
            reason = 'warming up',
            alert_code = 'app.warming_up',
        })
        t.assert_equals(#box.info.config.alerts, 1)
        t.assert_items_include(box.info.config.alerts[1], {
            type = 'warn',
            message = 'Readiness health check "app" failed: warming up',
            alert_code = 'app.warming_up',
        })

        t.assert_equals(health.remove_health_check('app'), true)
        t.assert_equals(box.info.health.readiness.status, 'ready')
        t.assert_equals(box.info.config.alerts, {})

        t.assert_equals(box.ctl.liveness_probe('event-loop', function()
            return false, 'stalled'
        end), true)
        t.assert_equals(box.info.health.liveness.checks['event-loop'], {
            status = 'failed',
            reason = 'stalled',
            alert_code = 'health.liveness.event-loop',
        })
        t.assert_equals(#box.info.config.alerts, 1)
        t.assert_items_include(box.info.config.alerts[1], {
            type = 'warn',
            message = 'Liveness health check "event-loop" failed: stalled',
            alert_code = 'health.liveness.event-loop',
        })

        t.assert_equals(health.remove_liveness_probe('event-loop'), true)
        t.assert_equals(box.info.health.liveness.verdict, 'alive')
        t.assert_equals(box.info.config.alerts, {})
    end)

    cluster_obj:drop()
end

g.test_readiness_registry = function(cg)
    cg.server:exec(function()
        local health = require('internal.healthcheck')

        t.assert_equals(health.add_health_check('app', function()
            return false, 'warming up'
        end), true)

        local info = box.info.health.readiness
        t.assert_equals(info.status, 'not_ready')
        t.assert_equals(info.checks.app, {
            status = 'not_ready',
            reason = 'warming up',
            alert_code = 'health.readiness.app',
        })

        local ok, err = health.add_health_check('app', function()
            return true
        end)
        t.assert_equals(ok, false)
        t.assert_str_contains(err, 'already exists')

        t.assert_equals(health.remove_health_check('app'), true)
        t.assert_equals(box.info.health.readiness.status, 'ready')
    end)
end

g.test_liveness_probe = function(cg)
    cg.server:exec(function()
        local ok, err = box.ctl.liveness_probe('event-loop',
            function()
                return false, 'stalled'
            end)
        t.assert_equals(ok, true)
        t.assert_equals(err, nil)

        local liveness = box.info.health.liveness
        t.assert_equals(liveness.verdict, 'restart_required')
        t.assert_equals(liveness.reason, 'stalled')
        t.assert_equals(liveness.checks['event-loop'], {
            status = 'failed',
            reason = 'stalled',
            alert_code = 'health.liveness.event-loop',
        })

        local health = require('internal.healthcheck')
        t.assert_equals(health.remove_liveness_probe('event-loop'), true)
        t.assert_equals(box.info.health.liveness.verdict, 'alive')
    end)
end

g.test_check_errors_are_isolated = function(cg)
    cg.server:exec(function()
        local health = require('internal.healthcheck')
        t.assert_equals(health.add_health_check('broken', function()
            error('bad check', 0)
        end), true)

        local info = box.info.health.readiness
        t.assert_equals(info.status, 'not_ready')
        t.assert_equals(info.checks.broken.status, 'not_ready')
        t.assert_equals(info.checks.broken.alert_code,
                        'health.readiness.broken')
        t.assert_str_contains(info.checks.broken.reason, 'bad check')

        t.assert_equals(health.remove_health_check('broken'), true)
    end)
end

g.test_invalid_check_result = function(cg)
    cg.server:exec(function()
        local health = require('internal.healthcheck')
        t.assert_equals(health.add_health_check('invalid', function()
            return nil
        end), true)

        local info = box.info.health.readiness
        t.assert_equals(info.status, 'not_ready')
        t.assert_equals(info.checks.invalid, {
            status = 'not_ready',
            reason = 'health check must return true or false, <string>',
            alert_code = 'health.readiness.invalid',
        })

        t.assert_equals(health.remove_health_check('invalid'), true)
    end)
end
