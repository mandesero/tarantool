local json = require('json')
local fio = require('fio')

local ok, wasm = pcall(require, "wasm")
if not ok then
    return {
        new = function()
            error("WASM module not found. Please ensure 'wasm.so' is available.")
        end
    }
end

local M = {}

local function sanitize(name)
    return name:gsub("-", "_")
end

local function read_file(path)
    local f = assert(io.open(path, "r"))
    local content = f:read("*a")
    f:close()
    return content
end

local function exists(file)
    local ok, err, code = os.rename(file, file)
    if not ok then
       if code == 13 then
          return true
       end
    end
    return ok, err
 end

 local function prepare_config(config)
    local cfg = {}

    cfg.inherit_env     = config.env.inherit_env
    cfg.inherit_args    = config.args.inherit_args
    cfg.inherit_stdin   = config.stdio.inherit_stdin
    cfg.inherit_stdout  = config.stdio.inherit_stdout
    cfg.inherit_stderr  = config.stdio.inherit_stderr
    cfg.inherit_network = config.network.inherit_network

    cfg.env = {}
    for k, v in pairs(config.env.vars or {}) do
        cfg.env[k] = v
    end

    cfg.args = {}
    for _, v in ipairs(config.args.value or {}) do
        table.insert(cfg.args, v)
    end

    cfg.stdin  = config.stdio.stdin_path
    cfg.stdout = config.stdio.stdout_path
    cfg.stderr = config.stdio.stderr_path

    cfg.allow_ip_name_lookup = config.network.allow_ip_name_lookup
    cfg.allow_tcp            = config.network.allow_tcp
    cfg.allow_udp            = config.network.allow_udp
    cfg.allowed_ips          = config.network.allowed_ips or {}
    cfg.allowed_ports        = config.network.allowed_ports or {}

    cfg.memory_limit     = config.limits.memory_limit
    cfg.max_instructions = config.limits.max_instructions

    cfg.preopened_dirs = {}
    for _, d in ipairs(config.fs.preopened_dirs or {}) do
        table.insert(cfg.preopened_dirs, {
            host_path  = d.host_path,
            guest_path = d.guest_path,
            perms      = d.perms,
        })
    end

    return cfg
end


local function make_func(uid, full_name, short_name, info, iface_full, iface_short)
    local wrapper = {}

    wrapper._args = info.params
    wrapper._result = info.result
    wrapper._doc = info.doc
    wrapper._interface = iface_full
    wrapper._interface_short = iface_short
    wrapper._short_name = short_name
    wrapper._full_name = full_name

    function wrapper:help()
        local args = {}
        for _, pair in ipairs(self._args or {}) do
            table.insert(args, pair[1] .. ": " .. pair[2])
        end

        local sig = string.format("%s(%s) -> %s",
            sanitize(self._short_name),
            table.concat(args, ", "),
            table.concat(self._result or {}, ", "))

        if self._doc then
            print(self._doc)
        end

        if self._interface and self._interface_short then
            print(string.format("%s\tfrom iface.%s (\"%s\")", sig, self._interface_short, self._interface))
        else
            print(sig)
        end
    end

    return setmetatable(wrapper, {
        __call = function(_, _, ...)
            return wasm.call(uid, full_name, ...)
        end
    })
end

function M:new(opts)
    assert(opts.wasm or opts.dir, "must provide either `wasm` or `dir`")

    local meta = {
        wasm_path = opts.wasm,
        world = nil,
        lang = nil,
        wit_path = nil,
        source = nil,
        component_name = opts.name,
    }

    if opts.dir then
        local tarawasm_path = fio.pathjoin(opts.dir, "tarawasm.json")
        local content = read_file(tarawasm_path)
        local parsed = assert(json.decode(content))

        meta.world = parsed.world
        meta.lang = parsed.lang
        meta.wit_path = fio.pathjoin(opts.dir, parsed.wit_path)
        meta.source = fio.pathjoin(opts.dir, parsed.src_file)
        meta.wasm_wit_path = fio.pathjoin(opts.dir, parsed.wasm_file)

        if opts.wasm then
            if exists(opts.wasm) then
                meta.wasm_path = opts.wasm
            else
                meta.wasm_path = fio.pathjoin(opts.dir, opts.wasm)
            end
        else
            meta.wasm_path = fio.pathjoin(opts.dir, parsed.world .. ".wasm")
        end
    elseif not meta.wasm_path then
        error("WASM path is not resolved")
    end

    local config = prepare_config(opts.config or {})

    local uid = wasm.load(meta.wasm_path, config)
    local exports = wasm.exports(uid)

    local iface = {}
    local lookup = {}
    local name_counter = {}

    local public = {
        iface = iface
    }

    local internal = {
        __uid = uid,
        __wasm_exports = exports,
        __lookup = lookup,
        __meta = meta,
        __config = config,
        __handle = nil,
    }

    for key, value in pairs(exports) do
        if value.params then
            local sanitized = sanitize(key)
            public[sanitized] = make_func(uid, key, sanitized, value, nil, nil)
            lookup[sanitized] = public[sanitized]
        else
            local short = key:match(".*/([%w%-_]+)@") or key
            short = sanitize(short)
            if public[short] then
                name_counter[short] = (name_counter[short] or 0) + 1
                short = short .. "_" .. tostring(name_counter[short])
            end

            iface[short] = {}

            for fname, finfo in pairs(value) do
                local full = key .. "::" .. fname
                local sanitized_fname = sanitize(fname)
                local f = make_func(uid, full, sanitized_fname, finfo, key, short)
                iface[short][sanitized_fname] = f
                lookup["iface." .. short .. "." .. sanitized_fname] = f
            end
        end
    end

    function public:help()
        print("Exported functions:\n")
        for _, f in pairs(internal.__lookup) do
            if type(f) == "table" and f.help and getmetatable(f) and getmetatable(f).__call then
                f:help()
            end
        end
    end

    function public:meta()
        return internal.__meta
    end

    function public:run()
        internal.__handle = wasm.run(internal.__uid)
    end

    function public:join()
        local h = internal.__handle
        if not h then
            error("No handle to join. Did you run the component?")
        end
        return wasm.join(h)
    end

    function public:batch(call_list)
        local batch_calls = {}

        for i, entry in ipairs(call_list) do
            local func = entry[1]
            if not getmetatable(func) or not getmetatable(func).__call or not func._full_name then
                error(string.format("entry[%d][1] must be a valid wasm function", i))
            end

            local found_name = func._full_name
            local args = {}
            for j = 2, #entry do
                table.insert(args, entry[j])
            end

            table.insert(batch_calls, {found_name, unpack(args)})
        end

        return wasm.batch(internal.__uid, batch_calls)
    end

    function public:drop()
        local name = internal.__meta.component_name

        if box.wasm.get(name) == nil then
            error("No such component: " .. name, 2)
        end

        wasm.drop(internal.__uid)
        box.wasm.components[name] = nil
        return true
    end

    return setmetatable(public, {
        __index = function(_, k)
            return internal[k]
        end
    })
end

function M.load_components(components)
    local registry = {}
    if type(components) ~= 'table' then
        return registry
    end

    for name, opts in pairs(components) do
        local comp_opts = {
            name = name,
            config = opts,
        }

        if fio.path.is_dir(opts.path) then
            comp_opts.dir = opts.path
        else
            comp_opts.wasm = opts.path
        end

        local comp = M:new(comp_opts)
        registry[name] = comp
        if opts.autorun then
            comp:run()
        end
    end

    return registry
end

return M
