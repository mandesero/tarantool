local M = {}
local wasm = require('wasm')
local json = require('json')
local fio = require('fio')

getmetatable("").__index.split = function(str, sep)
    sep = sep or "%s"
    local t = {}
    for part in str:gmatch("([^" .. sep .. "]+)") do
        table.insert(t, part)
    end
    return t
end

local function read_file(path)
    local f = assert(io.open(path, 'r'))
    local content = f:read('*a')
    f:close()
    return content
end

-- parse JSON exports
local function parse_exports(exports_path)
    local content = read_file(exports_path)
    local ok, exports = pcall(json.decode, content)
    if not ok then
        error("failed to parse exports JSON: " .. tostring(exports))
    end

    local map = {}
    local fnmap = {}  -- lua_name to original_name

    for _, func in ipairs(exports) do
        local lua_name = func.name:gsub("-", "_")
        fnmap[lua_name] = func.name
        map[lua_name] = {
            args = func.params,
            result = func.result,
            doc = func.doc,
        }
    end
    return map, fnmap
end

-- parse tarawasm.json to get the world name
local function parse_world_name(path)
    local content = read_file(path)
    local ok, data = pcall(json.decode, content)
    if not ok then
        error("failed to parse tarawasm.json: " .. tostring(data))
    end
    return data.world
end

function M:new(opts)
    assert(opts.dir, "component dir must be provided")

    local tarawasm_json_path = fio.pathjoin(opts.dir, "tarawasm.json")
    local world = opts.world or parse_world_name(tarawasm_json_path)
    local wasm_file = opts.wasm or (world .. ".wasm")
    local exports_json_path = opts.exports or (fio.pathjoin(opts.dir, "/component.exports.json"))

    local m = wasm.load(fio.pathjoin(opts.dir, wasm_file))
    local exports, fnmap = parse_exports(exports_json_path)

    local obj = {
        __module = m,
        __exports = exports
    }

    for lua_name, _ in pairs(exports) do
        obj[lua_name] = function(self, ...)
            return wasm.call(self.__module, fnmap[lua_name], ...)
        end
    end

    function obj:run()
        self.__handle = wasm.run(self.__module)
    end

    function obj:join()
        if not self.__handle then
            error("No handle to join. Did you run the component?")
        end
        return wasm.join(self.__handle)
    end

    function obj:help(name)
        local function print_func(fname, def)
            local args = {}
            for _, pair in ipairs(def.args or {}) do
                table.insert(args, pair[1] .. ": " .. pair[2])
            end

            local signature = string.format("%s(%s) -> %s",
                fname, table.concat(args, ", "), def.result or "void")

            if def.doc then
                for _, line in ipairs(def.doc:split("\n")) do
                    print(line)
                end
            end
            print(signature)
            print("")
        end

        if name == nil then
            print("Exported functions:")
            print("")
            for fname, def in pairs(self.__exports) do
                print_func(fname, def)
            end
        else
            local def = self.__exports[name]
            if def then
                print_func(name, def)
            else
                print("No such exported function: " .. name)
            end
        end
    end

    return obj
end

return M
