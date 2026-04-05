-- init.lua
box.cfg({
    listen = 3301,
    memtx_memory = 4 * 1024 * 1024 * 1024, -- 4GB
    readahead = 1024 * 1024,
    net_msg_max = 4096
})

-- Создаем пространство KV
local space = box.schema.space.create('KV', {
    if_not_exists = true,
    engine = 'memtx'
})

space:format({
    {name = 'key', type = 'string'},
    {name = 'value', type = 'varbinary', is_nullable = true}
})

space:create_index('primary', {
    type = 'TREE',
    parts = {'key'},
    unique = true,
    if_not_exists = true
})

print("=================================")
print("Tarantool ready!")
print("Space KV created")
print("=================================")

-- Оставляем консоль открытой
require('console').start()