local utils = require('internal.utils')

local function get_config()
    local config = package.loaded.config
    if type(config) ~= 'table' or type(config.info) ~= 'function' then
        return nil
    end
    return config
end

local health_alerts_namespace

local function get_health_alerts_namespace()
    if health_alerts_namespace == nil then
        local config = get_config()
        if config ~= nil and config._aboard ~= nil then
            health_alerts_namespace = config._aboard:new_namespace('health')
        end
    end

    return health_alerts_namespace
end

local health_checks = {
    liveness = {
        alert_prefix = 'Liveness',
        alerts = {},
    },
    readiness = {
        alert_prefix = 'Readiness',
        alerts = {},
    },
}

-- {{{ Alert helpers

local function alert_key(alert_code)
    return alert_code:gsub(':', '.')
end

local function alert_message(kind, name, check)
    local health_check = health_checks[kind]
    assert(health_check ~= nil)

    local reason = check.reason or check.status

    return ('%s health check %q failed: %s'):format(health_check.alert_prefix,
                                                    name, reason)
end

local function set_alert(kind, name, message, code)
    assert(health_checks[kind] ~= nil)

    local namespace = get_health_alerts_namespace()
    if namespace == nil then
        return
    end

    local key = alert_key(code)
    local issued = health_checks[kind].alerts[name]

    if issued ~= nil and issued.message == message and issued.key == key then
        return
    end

    if issued ~= nil and issued.key ~= key then
        namespace:unset(issued.key)
    end

    namespace:set(key, {
        message = message,
        alert_code = code,
    })

    health_checks[kind].alerts[name] = {
        key = key,
        message = message,
    }
end

local function unset_alert(kind, name)
    assert(health_checks[kind] ~= nil)

    local issued = health_checks[kind].alerts[name]
    if issued == nil then
        return
    end

    local namespace = get_health_alerts_namespace()
    if namespace ~= nil then
        namespace:unset(issued.key)
    end
    health_checks[kind].alerts[name] = nil
end

local function sync_alerts(kind, results, ok_status)
    assert(health_checks[kind] ~= nil)

    local failed = {}

    for name, check in pairs(results) do
        if check.status ~= ok_status then
            failed[name] = true
            set_alert(kind, name, alert_message(kind, name, check),
                      check.alert_code)
        end
    end

    for name, _ in pairs(health_checks[kind].alerts) do
        if not failed[name] then
            unset_alert(kind, name)
        end
    end
end

-- }}} Alert helpers

local checks = {
    liveness = {},
    readiness = {},
}

local function default_alert_code(kind, name)
    return ('health.%s.%s'):format(kind, name)
end

local HEALTHCHECK_OPTION_TYPES = {
    if_exists = 'boolean',
    if_not_exists = 'boolean',
    alert_code = 'string',
}

local function add_check(kind, name, fn, opts)
    opts = opts or {}

    utils.check_param(name, 'name', 'string')
    utils.check_param(fn, 'fn', 'function')
    utils.check_param_table(opts, HEALTHCHECK_OPTION_TYPES)

    local registry = checks[kind]
    if registry[name] ~= nil and not opts.if_not_exists then
        return false, ('health check %q already exists'):format(name)
    end

    if registry[name] == nil then
        registry[name] = {
            fn = fn,
            alert_code = opts.alert_code or default_alert_code(kind, name),
        }
    end

    return true
end

local function remove_check(kind, name, opts)
    opts = opts or {}

    utils.check_param(name, 'name', 'string')
    utils.check_param_table(opts, HEALTHCHECK_OPTION_TYPES)

    local registry = checks[kind]
    if registry[name] == nil and not opts.if_exists then
        return false, ('health check %q does not exist'):format(name)
    end

    unset_alert(kind, name)
    registry[name] = nil

    return true
end

local function evaluate(fn, ok_status, fail_status, alert_code)
    local ok, res, reason = pcall(fn)

    if not ok then
        return {
            status = fail_status,
            reason = tostring(res),
            alert_code = alert_code,
        }
    end

    if res == true then
        return { status = ok_status }
    end

    local failed = {
        status = fail_status,
        alert_code = alert_code,
    }

    if res == false and type(reason) == 'string' then
        failed.reason = reason
    else
        failed.reason = 'health check must return true or false, <string>'
    end

    return failed
end

local function evaluate_registry(kind, ok_status, fail_status)
    local res = {}
    for name, check in pairs(checks[kind]) do
        res[name] = evaluate(check.fn, ok_status, fail_status,
                             check.alert_code)
    end
    return res
end

local function liveness()
    local registry_checks = evaluate_registry('liveness', 'ok', 'failed')
    sync_alerts('liveness', registry_checks, 'ok')

    local res = {
        verdict = 'alive',
        checks = registry_checks,
    }

    for _, check in pairs(res.checks) do
        if check.status == 'failed' then
            res.verdict = 'restart_required'
            if res.reason == nil then
                res.reason = check.reason
            end
        end
    end
    return res
end

local function readiness()
    local registry_checks = evaluate_registry('readiness', 'ready',
                                              'not_ready')
    sync_alerts('readiness', registry_checks, 'ready')

    local res = {
        status = 'ready',
        checks = registry_checks,
    }

    for _, check in pairs(res.checks) do
        if check.status ~= 'ready' then
            res.status = 'not_ready'
            break
        end
    end
    return res
end

local health = {}

function health.add_health_check(name, fn, opts)
    return add_check('readiness', name, fn, opts)
end

function health.remove_health_check(name, opts)
    return remove_check('readiness', name, opts)
end

function health.liveness_probe(name, fn)
    return add_check('liveness', name, fn)
end

function health.remove_liveness_probe(name, opts)
    return remove_check('liveness', name, opts)
end

function health.liveness()
    return liveness()
end

function health.readiness()
    return readiness()
end

function health.info()
    return {
        liveness = liveness(),
        readiness = readiness(),
    }
end

return health
