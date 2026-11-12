-- Single-instance Redis authority. Prefixes are isolated from cancellation keys.
-- All decisions and multi-scope mutations occur in this one atomic script.
local p = cjson.decode(ARGV[1])
local action = p.action
local function valid_number(n, min, max)
    return type(n) == 'number' and n >= min and n <= max and n == math.floor(n)
end
if not valid_number(p.lease_ms, 100, 60000) or
   not valid_number(p.recovery_ms, p.lease_ms, 120000) or
   type(p.member) ~= 'string' or #p.member > 128 or #p.member == 0 or
   type(p.epoch) ~= 'string' or #p.epoch > 128 or
   not (action == 'epoch' or action == 'acquire' or action == 'renew' or action == 'release' or action == 'rate') then
    return redis.error_reply('invalid shared budget request')
end
local scopes = p.scopes
if type(scopes) ~= 'table' or #scopes > 8 or (action ~= 'epoch' and #scopes == 0) then
    return redis.error_reply('invalid scope count')
end
local seen = {}
for _, s in ipairs(scopes) do
    if type(s.key) ~= 'string' or #s.key > 512 or not string.find(s.key, '^lakeon:security:v1:') or seen[s.key] then
        return redis.error_reply('invalid scope key')
    end
    seen[s.key] = true
    local keytype = redis.call('TYPE', s.key).ok
    if action == 'rate' then
        if not valid_number(s.burst, 1, 1000000) or type(s.rate) ~= 'number' or s.rate <= 0 or s.rate > 1000000 or (keytype ~= 'none' and keytype ~= 'hash') then
            return redis.error_reply('invalid rate policy or key type')
        end
    elseif action ~= 'epoch' then
        if not valid_number(s.limit, 1, 1000000) or (keytype ~= 'none' and keytype ~= 'zset') then
            return redis.error_reply('invalid connection policy or key type')
        end
    end
end
local ts = redis.call('TIME')
local now = tonumber(ts[1]) * 1000 + math.floor(tonumber(ts[2]) / 1000)
local epoch = redis.call('HGET', KEYS[1], 'id')
if not epoch then
    epoch = p.epoch .. ':' .. tostring(now)
    redis.call('HSET', KEYS[1], 'id', epoch, 'ready_at', now + p.recovery_ms, 'recovery_ms', p.recovery_ms)
end
local function result(status, extra)
    local r = extra or {}
    r.status = status
    r.epoch = epoch
    return cjson.encode(r)
end
if tonumber(redis.call('HGET', KEYS[1], 'recovery_ms')) ~= p.recovery_ms then
    return result('policy_mismatch')
end
if now < tonumber(redis.call('HGET', KEYS[1], 'ready_at')) then
    return result('recovering')
end
if p.expected_epoch and p.expected_epoch ~= cjson.null and p.expected_epoch ~= epoch then return result('epoch_changed') end
if action == 'epoch' then return result('ok') end
if action == 'rate' then
    local balances = {}
    for i, s in ipairs(scopes) do
        local values = redis.call('HMGET', s.key, 'tokens', 'at')
        local tokens = tonumber(values[1]) or s.burst
        local at = tonumber(values[2]) or now
        tokens = math.min(s.burst, tokens + math.max(0, now - at) * s.rate / 1000)
        if tokens < 1 then return result('limited') end
        balances[i] = tokens - 1
    end
    for i, s in ipairs(scopes) do
        redis.call('HSET', s.key, 'tokens', balances[i], 'at', now)
        redis.call('PEXPIRE', s.key, math.ceil(s.burst / s.rate * 1000) + 1000)
    end
    return result('ok')
end
if action == 'release' then
    for _, s in ipairs(scopes) do redis.call('ZREM', s.key, p.member) end
    return result('ok')
end
local all_held = true
local earliest_expiry = now + p.lease_ms
for _, s in ipairs(scopes) do
    redis.call('ZREMRANGEBYSCORE', s.key, '-inf', now)
    local expires = tonumber(redis.call('ZSCORE', s.key, p.member))
    if not expires then all_held = false else earliest_expiry = math.min(earliest_expiry, expires) end
end
if action == 'renew' and not all_held then return result('lost') end
-- A retry after a lost response must not extend the original lease silently.
if action == 'acquire' and all_held then
    return result('ok', {remaining_ms = earliest_expiry - now})
end
if action == 'acquire' then
    for _, s in ipairs(scopes) do
        local own = redis.call('ZSCORE', s.key, p.member) and 1 or 0
        if redis.call('ZCARD', s.key) - own >= s.limit then return result('full') end
    end
end
for _, s in ipairs(scopes) do
    redis.call('ZADD', s.key, now + p.lease_ms, p.member)
    redis.call('PEXPIRE', s.key, p.recovery_ms * 2)
end
return result('ok', {remaining_ms = p.lease_ms})
