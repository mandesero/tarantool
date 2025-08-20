local luawasm = require('luawasm')

local function apply(config)
    local cfg = config._configdata:get('wasm.components', {use_default = true}) or {}
    box.wasm = box.wasm or {}
    box.wasm.components = luawasm.load_components(cfg)
    function box.wasm.get(name)
        return box.wasm.components[name]
    end
end

return {
    name = 'wasm',
    apply = apply,
}
