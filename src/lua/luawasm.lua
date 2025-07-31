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

local function make_func(module, full_name, short_name, info, iface_full, iface_short)
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
            return wasm.call(module, full_name, ...)
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

    local m = wasm.load(meta.wasm_path)
    local exports = wasm.exports(m)

    local iface = {}
    local lookup = {}
    local name_counter = {}

    local public = {
        iface = iface
    }

    local internal = {
        __module = m,
        __wasm_exports = exports,
        __lookup = lookup,
        __meta = meta
    }

    for key, value in pairs(exports) do
        if value.params then
            local sanitized = sanitize(key)
            public[sanitized] = make_func(m, key, sanitized, value, nil, nil)
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
                local f = make_func(m, full, sanitized_fname, finfo, key, short)
                iface[short][sanitized_fname] = f
                lookup["iface." .. short .. "." .. sanitized_fname] = f
            end
        end
    end

    function public:help()
        print("Exported functions:\n")
        for name, f in pairs(internal.__lookup) do
            if type(f) == "table" and f.help and getmetatable(f) and getmetatable(f).__call then
                f:help()
            end
        end
    end

    function public:meta()
        return internal.__meta
    end

    function public:run()
        self.__handle = wasm.run(internal.__module)
    end

    function public:join()
        if not self.__handle then
            error("No handle to join. Did you run the component?")
        end
        return wasm.join(self.__handle)
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
    
        return wasm.batch(internal.__module, batch_calls)
    end

    return setmetatable(public, {
        __index = function(_, k)
            return internal[k]
        end
    })
end

return M
