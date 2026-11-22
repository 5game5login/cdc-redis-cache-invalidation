-- update_if_newer.lua
-- Atomically apply a change to a cache entry ONLY if its version is newer
-- than the version already recorded for that key.
--
-- A version is the pair (epoch, lsn), compared lexicographically:
--   epoch  operator-controlled generation number. Bump it whenever WAL
--          positions can go backwards: restore from backup/PITR, rebuild,
--          major-version upgrade that recreates the slot. Old-epoch events
--          then always lose to new-epoch events.
--   lsn    Debezium source.lsn of the change (monotonic per row, because
--          row locks serialise writers).
-- Both are unsigned decimal strings: PG LSNs are 64-bit, beyond Lua's 2^53
-- exact-integer range, so they are compared as strings, never as numbers.
--
-- KEYS[1]  cache key (Hash), e.g. "product:42"  (ignored for op=truncate)
-- KEYS[2]  table truncate watermark (Hash {epoch, lsn}), e.g. "cdc:truncate:product"
-- ARGV[1]  op: "set" | "del" | "truncate" | "fill"
-- ARGV[2]  payload JSON (set/fill only)
-- ARGV[3]  epoch
-- ARGV[4]  lsn (ignored for fill)
-- ARGV[5]  TTL seconds (0 = none; ignored for truncate)
--
-- Returns 1 if applied, 0 if rejected as stale/duplicate/already-present.
--
-- Semantics
--   set/del   applied iff version > stored version AND version > watermark.
--             Deletes write a tombstone (deleted=1) instead of DEL so a delayed
--             older UPDATE cannot resurrect the row.
--   truncate  raises the table watermark. Entries at or below it become
--             invisible to readers (see Invalidator.get) and can never be
--             overwritten by older events; they age out via TTL.
--   fill      read-through population. Writes only if no valid entry exists,
--             stamped with version max((epoch, 0), watermark) and fill=1, so
--             ANY real CDC event for the key supersedes it. A reader cannot
--             know the true version of what it read (an uncommitted change
--             may already be in WAL), so it must never claim a real LSN.

local function norm(s, name)
  if type(s) ~= 'string' or not string.match(s, '^%d+$') then
    return nil, redis.error_reply('ERR ' .. name .. ' must be an unsigned decimal string')
  end
  return (string.gsub(s, '^0+(%d)', '%1'))
end

local function gt(a, b)              -- decimal string a > b
  if #a ~= #b then return #a > #b end
  return a > b
end

local function vgt(e1, l1, e2, l2)   -- (e1,l1) > (e2,l2)
  if e1 ~= e2 then return gt(e1, e2) end
  return gt(l1, l2)
end

local key, wm_key = KEYS[1], KEYS[2]
local op, payload = ARGV[1], ARGV[2]
local ttl = tonumber(ARGV[5]) or 0
local epoch, err = norm(ARGV[3], 'epoch')
if not epoch then return err end

local wm = redis.call('HMGET', wm_key, 'epoch', 'lsn')
local has_wm = wm[2] ~= false
local wm_e, wm_l = wm[1] or '0', wm[2]

-- Is the stored entry still valid with respect to the truncate watermark?
local function stored_version()
  local s = redis.call('HMGET', key, 'epoch', 'lsn', 'fill')
  if not s[2] then return nil end
  local se, sl, sfill = s[1] or '0', s[2], s[3] == '1'
  if has_wm then
    local above = vgt(se, sl, wm_e, wm_l)
    local fill_at_wm = sfill and se == wm_e and sl == wm_l
    if not above and not fill_at_wm then return nil end  -- pre-truncate leftover
  end
  return se, sl
end

local function write(fields)
  redis.call('DEL', key)
  redis.call('HSET', key, unpack(fields))
  if ttl > 0 then redis.call('EXPIRE', key, ttl) end
end

if op == 'fill' then
  if stored_version() then return 0 end
  local fe, fl = epoch, '0'
  if has_wm and vgt(wm_e, wm_l, fe, fl) then fe, fl = wm_e, wm_l end
  write({'epoch', fe, 'lsn', fl, 'payload', payload, 'deleted', '0', 'fill', '1'})
  return 1
end

local lsn
lsn, err = norm(ARGV[4], 'lsn')
if not lsn then return err end

if has_wm and not vgt(epoch, lsn, wm_e, wm_l) then
  return 0                                   -- at/below truncate watermark
end

if op == 'truncate' then
  redis.call('HSET', wm_key, 'epoch', epoch, 'lsn', lsn)
  return 1
end

local se, sl = stored_version()
if se and not vgt(epoch, lsn, se, sl) then
  return 0                                   -- stale or duplicate
end

if op == 'set' then
  write({'epoch', epoch, 'lsn', lsn, 'payload', payload, 'deleted', '0'})
elseif op == 'del' then
  write({'epoch', epoch, 'lsn', lsn, 'deleted', '1'})
else
  return redis.error_reply('ERR op must be set, del, truncate or fill')
end
return 1
