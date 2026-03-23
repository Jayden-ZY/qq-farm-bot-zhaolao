/**

 */

const fs = require('fs');
const path = require('path');
const { CONFIG, PlantPhase, PHASE_NAMES } = require('./config');
const { types } = require('./proto');
const { sendMsgAsync, getUserState, networkEvents, getAsyncSendLoad } = require('./network');
const { toLong, toNum, getServerTimeSec, log, logWarn, sleep, sanitizeLogText } = require('./utils');
const { getCurrentPhase, setOperationLimitsCallback } = require('./farm');
const { getPlantName, getPlantById } = require('./gameConfig');

const _dataDir = process.env.QQ_FARM_DATA_DIR || null;
const WEBUI_CONFIG_PATH = _dataDir
    ? path.join(_dataDir, 'config.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/config.json');
const WEBUI_FRIENDS_PATH = _dataDir
    ? path.join(_dataDir, 'friends.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/friends.json');
const WEBUI_FRIEND_STATS_PATH = _dataDir
    ? path.join(_dataDir, 'friend-stats.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/friend-stats.json');
const SEED_SHOP_DATA_PATH = path.resolve(__dirname, '../tools/seed-shop-merged-export.json');
let webuiConfigMtimeMs = -1;
let webuiConfigCache = null;
let seedRequiredLevelMap = null;
let lastWebuiFriendsWriteErrorMsg = '';
let lastWebuiFriendsWriteErrorAt = 0;
let friendStatsCache = null;
let friendStatsDirty = false;
let lastWebuiFriendStatsWriteErrorMsg = '';
let lastWebuiFriendStatsWriteErrorAt = 0;


let isCheckingFriends = false;
let isFirstFriendCheck = true;
let friendCheckTimer = null;
let friendLoopRunning = false;
let lastResetDate = '';
let friendActiveStart = '';
let friendActiveEnd = '';
let friendActiveAllDay = false;
let lastWindowSkipLogAt = 0;
let friendApplyActiveStart = '';
let friendApplyActiveEnd = '';
let friendApplyAllDay = true;
let lastApplyWindowSkipLogAt = 0;
let friendActionSteal = true;
let friendActionCare = true;
let friendActionPrank = false;
let friendStealLevelThreshold = 0;
let friendStealEnabledMap = {};
let friendStealEnabledMapSig = '{}';
let friendAutoDeleteNoStealEnabled = false;
let friendAutoDeleteNoStealDays = 7;
let lastDailyActionAutoEnableDate = '';
let friendPerformanceMode = 'standard';
let friendActionDelayMs = 100;
let friendBetweenFriendDelayMs = 500;
let friendLoopTimeoutBackoffMs = 0;
let friendNoActionStreak = 0;
let lastFriendAutoDeleteNoStealSweepAt = 0;
const LOOP_TIMEOUT_BACKOFF_BASE_MS = 1200;
const LOOP_TIMEOUT_BACKOFF_MAX_MS = 8000;
const BERSERK_MIN_LOOP_WAIT_MS = 420;
const FRIEND_NETWORK_GUARD_MAX_MS = 2600;
const FRIEND_MAX_COUNT = 511;
const FRIEND_MANUAL_REQUEST_MAX_AGE_MS = 60 * 60 * 1000;
const FRIEND_AUTO_DELETE_NO_STEAL_SWEEP_INTERVAL_MS = 30 * 60 * 1000;
const pendingFriendApplications = new Map(); // gid -> name
const pendingFriendDeleteRequests = [];
let isProcessingPendingFriendApplications = false;
let isProcessingPendingFriendDeleteRequests = false;
let lastFriendApplyFullLogAt = 0;

const FRIEND_PERFORMANCE_PROFILES = {
    standard: {
        friendIntervalSec: 1,
        actionDelayMs: 100,
        betweenFriendDelayMs: 500,
    },
    retire: {
        friendIntervalSec: 5,
        actionDelayMs: 180,
        betweenFriendDelayMs: 900,
    },
    berserk: {
        friendIntervalSec: 0,
        actionDelayMs: 85,
        betweenFriendDelayMs: 280,
    },
};

function getFriendNetworkGuardWaitMs() {
    const load = getAsyncSendLoad();
    if (!load) return 0;

    const pending = Math.max(0, Number(load.pending) || 0);
    const penaltyMs = Math.max(0, Number(load.penaltyMs) || 0);
    const timeoutStreak = Math.max(0, Number(load.timeoutStreak) || 0);
    const dynamicGapMs = Math.max(0, Number(load.dynamicGapMs) || 0);

    if (pending <= 0 && penaltyMs <= 0 && timeoutStreak <= 0) return 0;

    const guardMs = Math.max(
        Math.floor(dynamicGapMs * 2),
        penaltyMs + pending * 340 + timeoutStreak * 520
    );
    return Math.min(guardMs, FRIEND_NETWORK_GUARD_MAX_MS);
}


const expTracker = new Map();
const expExhausted = new Set();





const operationLimits = new Map();


const OP_NAMES = {
    10001: 'STEAL',
    10002: 'WATER',
    10003: 'PUT_WEED',
    10004: 'PUT_BUG',
    10005: 'HELP_BUG',
    10006: 'HELP_WEED',
    10007: 'HELP_WATER',
    10008: 'PRANK',
};


const HELP_ONLY_WITH_EXP = true;


const ENABLE_PUT_BAD_THINGS = true;


function getSeedRequiredLevelMap() {
    if (seedRequiredLevelMap) return seedRequiredLevelMap;
    seedRequiredLevelMap = new Map();
    try {
        if (!fs.existsSync(SEED_SHOP_DATA_PATH)) return seedRequiredLevelMap;
        const parsed = JSON.parse(fs.readFileSync(SEED_SHOP_DATA_PATH, 'utf8'));
        const rows = Array.isArray(parsed) ? parsed : (parsed.rows || parsed.seeds || []);
        if (!Array.isArray(rows)) return seedRequiredLevelMap;

        for (const row of rows) {
            if (!row || typeof row !== 'object') continue;
            const seedId = Number(row.seedId || row.seed_id);
            if (!Number.isInteger(seedId) || seedId <= 0) continue;
            const levelNeed = Number(row.requiredLevel || row.required_level || 0);
            if (!Number.isFinite(levelNeed) || levelNeed < 0) continue;
            seedRequiredLevelMap.set(seedId, Math.floor(levelNeed));
        }
    } catch (e) {
        // ignore
    }
    return seedRequiredLevelMap;
}

function getPlantStealLevelNeed(plantId) {
    const plant = getPlantById(plantId);
    if (!plant || typeof plant !== 'object') return 0;
    const seedId = Number(plant.seed_id || plant.seedId || 0);
    if (!Number.isInteger(seedId) || seedId <= 0) return 0;
    const levelNeed = getSeedRequiredLevelMap().get(seedId);
    return Number.isInteger(levelNeed) && levelNeed >= 0 ? levelNeed : 0;
}

function normalizeFriendStealNameKey(name) {
    const text = String(name || '').trim().toLowerCase();
    if (!text) return '';
    return text.replace(/\s+/g, ' ');
}

function getFriendStealKey(friend) {
    const gid = toNum(friend && friend.gid);
    if (gid > 0) return String(gid);
    const nameKey = normalizeFriendStealNameKey(friend && friend.name);
    return nameKey ? `name:${nameKey}` : '';
}

function normalizeFriendStealEnabled(raw) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
    const map = {};
    let count = 0;
    for (const [k, v] of Object.entries(raw)) {
        if (count >= 10000) break;
        const key = String(k || '').trim();
        if (!key || key.length > 80) continue;
        map[key] = v !== false;
        count += 1;
    }
    return map;
}

function isFriendStealEnabled(friend) {
    const key = getFriendStealKey(friend);
    if (!key) return true;
    return friendStealEnabledMap[key] !== false;
}

async function getAllFriends() {
    const body = types.GetAllFriendsRequest.encode(types.GetAllFriendsRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.friendpb.FriendService', 'GetAll', body);
    return types.GetAllFriendsReply.decode(replyBody);
}

async function getFriendCapacityInfo() {
    const friendsReply = await getAllFriends();
    const friends = Array.isArray(friendsReply && friendsReply.game_friends) ? friendsReply.game_friends : [];
    const currentCount = friends.length;
    const remainingSlots = Math.max(0, FRIEND_MAX_COUNT - currentCount);
    return { currentCount, remainingSlots };
}

function normalizeFriendDeleteRequests(raw) {
    if (!Array.isArray(raw)) return [];
    const rows = [];
    const seen = new Set();
    for (const item of raw) {
        if (!item || typeof item !== 'object') continue;
        if (rows.length >= 50) break;
        const requestId = String(item.requestId || '').trim().slice(0, 80);
        const gid = toNum(item.gid);
        if (!requestId || gid <= 0 || seen.has(requestId)) continue;
        seen.add(requestId);
        rows.push({
            requestId,
            gid,
            name: String(item.name || '').trim().slice(0, 80),
            requestedAtMs: Math.max(0, Number(item.requestedAtMs) || 0),
        });
    }
    return rows;
}

function shouldIgnoreStaleFriendRequest(reqAtMs) {
    const n = Number(reqAtMs || 0);
    if (!Number.isFinite(n) || n <= 0) return false;
    return (Date.now() - n) > FRIEND_MANUAL_REQUEST_MAX_AGE_MS;
}

function enqueueFriendDeleteRequests(requests) {
    const rows = Array.isArray(requests) ? requests : [];
    if (rows.length <= 0) return [];

    const knownIds = new Set(pendingFriendDeleteRequests.map((item) => String(item.requestId || '').trim()));
    const knownGids = new Set(pendingFriendDeleteRequests.map((item) => toNum(item && item.gid)).filter((gid) => gid > 0));
    const added = [];
    for (const item of rows) {
        if (!item || typeof item !== 'object') continue;
        const requestId = String(item.requestId || '').trim();
        if (!requestId || knownIds.has(requestId)) continue;
        const gid = toNum(item.gid);
        if (gid <= 0 || knownGids.has(gid)) continue;
        const request = {
            requestId,
            gid,
            name: String(item.name || '').trim().slice(0, 80),
            requestedAtMs: Math.max(0, Number(item.requestedAtMs) || 0),
        };
        pendingFriendDeleteRequests.push(request);
        knownIds.add(requestId);
        knownGids.add(gid);
        added.push(request);
    }
    return added;
}

function removeFriendDeleteRequestsFromConfig(requestIds) {
    const ids = new Set((Array.isArray(requestIds) ? requestIds : [])
        .map((id) => String(id || '').trim())
        .filter(Boolean));
    if (ids.size <= 0) return;

    const current = readWebuiConfig() || {};
    const currentRows = normalizeFriendDeleteRequests(current.friendDeleteRequests);
    if (currentRows.length <= 0) return;
    const nextRows = currentRows.filter((item) => !ids.has(item.requestId));
    if (nextRows.length === currentRows.length) return;
    writeWebuiConfigPatch({ friendDeleteRequests: nextRows });
}

function parseRuntimeIsoToMs(value) {
    const text = String(value || '').trim();
    if (!text) return 0;
    const ms = Date.parse(text);
    return Number.isFinite(ms) ? ms : 0;
}

function maybeQueueAutoDeleteNoStealFriends(friends, myGid) {
    if (!friendAutoDeleteNoStealEnabled || !friendActionSteal) return;
    const days = Math.max(1, Math.floor(Number(friendAutoDeleteNoStealDays) || 0));
    if (days <= 0) return;

    const now = Date.now();
    if (lastFriendAutoDeleteNoStealSweepAt > 0 && (now - lastFriendAutoDeleteNoStealSweepAt) < FRIEND_AUTO_DELETE_NO_STEAL_SWEEP_INTERVAL_MS) {
        return;
    }
    lastFriendAutoDeleteNoStealSweepAt = now;

    const thresholdMs = days * 24 * 60 * 60 * 1000;
    const stats = getFriendStatsCache();
    const rows = Array.isArray(friends) ? friends : [];
    const requests = [];

    for (const raw of rows) {
        const friend = normalizeWebuiFriend(raw, myGid);
        if (!friend || !isFriendStealEnabled(friend)) continue;

        const key = getFriendStatsKey(friend);
        const row = key ? stats.friends[key] : null;
        const lastStealMs = parseRuntimeIsoToMs(row && row.lastStealAt);
        const firstSeenMs = parseRuntimeIsoToMs(row && row.firstSeenAt) || parseRuntimeIsoToMs(stats.created_at);
        const referenceMs = lastStealMs > 0 ? lastStealMs : firstSeenMs;
        if (referenceMs <= 0 || (now - referenceMs) < thresholdMs) continue;

        requests.push({
            requestId: `auto-no-steal:${friend.gid}`,
            gid: friend.gid,
            name: friend.name,
            requestedAtMs: now,
        });
    }

    const added = enqueueFriendDeleteRequests(requests);
    if (added.length > 0) {
        const names = added
            .map((item) => normalizeRuntimeText(item.name || `GID:${item.gid}`).slice(0, 24) || `GID:${item.gid}`)
            .join(', ');
        log('FRIEND_DELETE', `连续${days}天未成功偷菜，已加入自动删除 ${added.length} 人：${names}`);
    }
}

function normalizeWebuiFriend(friend, myGid) {
    if (!friend || typeof friend !== 'object') return null;
    const gid = toNum(friend.gid);
    if (!gid || (myGid > 0 && gid === myGid)) return null;
    const nick = String(friend.name || '').trim();
    const remark = String(friend.remark || '').trim();
    const name = remark || nick || `GID:${gid}`;
    const plant = friend.plant || null;
    return {
        gid,
        name,
        nick,
        remark,
        level: toNum(friend.level),
        preview: {
            stealPlantNum: plant ? toNum(plant.steal_plant_num) : 0,
            dryNum: plant ? toNum(plant.dry_num) : 0,
            weedNum: plant ? toNum(plant.weed_num) : 0,
            insectNum: plant ? toNum(plant.insect_num) : 0,
        },
    };
}

function writeWebuiFriendsSnapshot(friends, myGid) {
    try {
        const rows = (Array.isArray(friends) ? friends : [])
            .map((friend) => normalizeWebuiFriend(friend, myGid))
            .filter(Boolean);
        const payload = {
            version: 1,
            source: 'qq-farm-bot',
            updated_at: new Date().toISOString(),
            total: rows.length,
            friends: rows,
        };

        fs.mkdirSync(path.dirname(WEBUI_FRIENDS_PATH), { recursive: true });
        const tmpPath = `${WEBUI_FRIENDS_PATH}.tmp`;
        fs.writeFileSync(tmpPath, `${JSON.stringify(payload)}\n`, 'utf8');
        fs.renameSync(tmpPath, WEBUI_FRIENDS_PATH);
        lastWebuiFriendsWriteErrorMsg = '';
    } catch (e) {
        const now = Date.now();
        const msg = e && e.message ? e.message : String(e);
        if (msg !== lastWebuiFriendsWriteErrorMsg || (now - lastWebuiFriendsWriteErrorAt) > 60_000) {
            logWarn('WEBUI', '好友列表写入失败: ' + msg);
            lastWebuiFriendsWriteErrorMsg = msg;
            lastWebuiFriendsWriteErrorAt = now;
        }
    }
}

function createEmptyFriendStats() {
    const now = new Date().toISOString();
    return {
        version: 1,
        source: 'qq-farm-bot',
        created_at: now,
        updated_at: now,
        totals: {
            visits: 0,
            stealCount: 0,
            careCount: 0,
            prankCount: 0,
            failCount: 0,
        },
        friends: {},
    };
}

function sanitizeFriendStats(raw) {
    const base = createEmptyFriendStats();
    if (!raw || typeof raw !== 'object') return base;
    const next = {
        ...base,
        ...raw,
        totals: { ...base.totals, ...(raw.totals && typeof raw.totals === 'object' ? raw.totals : {}) },
        friends: {},
    };
    const rows = raw.friends && typeof raw.friends === 'object' ? raw.friends : {};
    for (const [k, v] of Object.entries(rows)) {
        if (!v || typeof v !== 'object') continue;
        next.friends[String(k)] = {
            gid: toNum(v.gid),
            name: String(v.name || '').trim(),
            nick: String(v.nick || '').trim(),
            remark: String(v.remark || '').trim(),
            firstSeenAt: String(v.firstSeenAt || raw.created_at || base.created_at || ''),
            level: toNum(v.level),
            visits: Math.max(0, toNum(v.visits)),
            stealCount: Math.max(0, toNum(v.stealCount)),
            careCount: Math.max(0, toNum(v.careCount)),
            prankCount: Math.max(0, toNum(v.prankCount)),
            failCount: Math.max(0, toNum(v.failCount)),
            previewStealNum: Math.max(0, toNum(v.previewStealNum)),
            previewDryNum: Math.max(0, toNum(v.previewDryNum)),
            previewWeedNum: Math.max(0, toNum(v.previewWeedNum)),
            previewInsectNum: Math.max(0, toNum(v.previewInsectNum)),
            lastVisitAt: String(v.lastVisitAt || ''),
            lastStealAt: String(v.lastStealAt || ''),
            lastFailAt: String(v.lastFailAt || ''),
            stealPlants: (v.stealPlants && typeof v.stealPlants === 'object') ? { ...v.stealPlants } : {},
        };
    }
    return next;
}

function getFriendStatsCache() {
    if (friendStatsCache) return friendStatsCache;
    try {
        if (fs.existsSync(WEBUI_FRIEND_STATS_PATH)) {
            const parsed = JSON.parse(fs.readFileSync(WEBUI_FRIEND_STATS_PATH, 'utf8'));
            friendStatsCache = sanitizeFriendStats(parsed);
            return friendStatsCache;
        }
    } catch (e) {
        // ignore and rebuild
    }
    friendStatsCache = createEmptyFriendStats();
    return friendStatsCache;
}

function markFriendStatsDirty() {
    friendStatsDirty = true;
    const stats = getFriendStatsCache();
    stats.updated_at = new Date().toISOString();
}

function flushFriendStatsIfDirty() {
    if (!friendStatsDirty) return;
    try {
        const stats = getFriendStatsCache();
        fs.mkdirSync(path.dirname(WEBUI_FRIEND_STATS_PATH), { recursive: true });
        const tmpPath = `${WEBUI_FRIEND_STATS_PATH}.tmp`;
        fs.writeFileSync(tmpPath, `${JSON.stringify(stats)}\n`, 'utf8');
        fs.renameSync(tmpPath, WEBUI_FRIEND_STATS_PATH);
        friendStatsDirty = false;
        lastWebuiFriendStatsWriteErrorMsg = '';
    } catch (e) {
        const now = Date.now();
        const msg = e && e.message ? e.message : String(e);
        if (msg !== lastWebuiFriendStatsWriteErrorMsg || (now - lastWebuiFriendStatsWriteErrorAt) > 60_000) {
            logWarn('WEBUI', '好友统计写入失败: ' + msg);
            lastWebuiFriendStatsWriteErrorMsg = msg;
            lastWebuiFriendStatsWriteErrorAt = now;
        }
    }
}

function getFriendStatsKey(friend) {
    const gid = toNum(friend && friend.gid);
    if (gid > 0) return String(gid);
    const name = String(friend && friend.name || '').trim();
    return name ? `name:${name}` : '';
}

function ensureFriendStatsRow(friend) {
    const key = getFriendStatsKey(friend);
    if (!key) return null;
    const stats = getFriendStatsCache();
    if (!stats.friends[key]) {
        stats.friends[key] = {
            gid: toNum(friend && friend.gid),
            name: String(friend && friend.name || '').trim(),
            nick: String(friend && friend.nick || '').trim(),
            remark: String(friend && friend.remark || '').trim(),
            firstSeenAt: new Date().toISOString(),
            level: toNum(friend && friend.level),
            visits: 0,
            stealCount: 0,
            careCount: 0,
            prankCount: 0,
            failCount: 0,
            previewStealNum: 0,
            previewDryNum: 0,
            previewWeedNum: 0,
            previewInsectNum: 0,
            lastVisitAt: '',
            lastStealAt: '',
            lastFailAt: '',
            stealPlants: {},
        };
    }
    if (!stats.friends[key].firstSeenAt) {
        stats.friends[key].firstSeenAt = String(stats.created_at || new Date().toISOString());
    }
    return stats.friends[key];
}

function syncFriendStatsFromOfficialList(friends, myGid) {
    const list = Array.isArray(friends) ? friends : [];
    for (const f of list) {
        const friend = normalizeWebuiFriend(f, myGid);
        if (!friend) continue;
        const row = ensureFriendStatsRow(friend);
        if (!row) continue;
        row.gid = friend.gid;
        row.name = friend.name;
        row.nick = friend.nick;
        row.remark = friend.remark;
        row.level = toNum(friend.level);
        row.previewStealNum = Math.max(0, toNum(friend.preview && friend.preview.stealPlantNum));
        row.previewDryNum = Math.max(0, toNum(friend.preview && friend.preview.dryNum));
        row.previewWeedNum = Math.max(0, toNum(friend.preview && friend.preview.weedNum));
        row.previewInsectNum = Math.max(0, toNum(friend.preview && friend.preview.insectNum));
    }
    markFriendStatsDirty();
}

function recordFriendVisitFailure(friend, errMsg) {
    const row = ensureFriendStatsRow(friend);
    if (!row) return;
    const stats = getFriendStatsCache();
    row.failCount += 1;
    row.lastFailAt = new Date().toISOString();
    stats.totals.failCount += 1;
    markFriendStatsDirty();
}

function recordFriendActionStats(friend, actionDetail, enteredFarm = true) {
    const row = ensureFriendStatsRow(friend);
    if (!row) return;
    const stats = getFriendStatsCache();
    const nowIso = new Date().toISOString();

    if (enteredFarm) {
        row.visits += 1;
        row.lastVisitAt = nowIso;
        stats.totals.visits += 1;
    }

    const steal = Math.max(0, toNum(actionDetail && actionDetail.steal));
    const careWater = Math.max(0, toNum(actionDetail && actionDetail.careWater));
    const careWeed = Math.max(0, toNum(actionDetail && actionDetail.careWeed));
    const careBug = Math.max(0, toNum(actionDetail && actionDetail.careBug));
    const prankBug = Math.max(0, toNum(actionDetail && actionDetail.prankBug));
    const prankWeed = Math.max(0, toNum(actionDetail && actionDetail.prankWeed));
    const care = careWater + careWeed + careBug;
    const prank = prankBug + prankWeed;

    if (steal > 0) {
        row.stealCount += steal;
        row.lastStealAt = nowIso;
        stats.totals.stealCount += steal;
    }
    if (care > 0) {
        row.careCount += care;
        stats.totals.careCount += care;
    }
    if (prank > 0) {
        row.prankCount += prank;
        stats.totals.prankCount += prank;
    }

    const plants = Array.isArray(actionDetail && actionDetail.stealPlants) ? actionDetail.stealPlants : [];
    for (const plant of plants) {
        const name = String(plant || '').trim();
        if (!name) continue;
        row.stealPlants[name] = Math.max(0, toNum(row.stealPlants[name])) + 1;
    }

    markFriendStatsDirty();
}



async function getApplications() {
    const body = types.GetApplicationsRequest.encode(types.GetApplicationsRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.friendpb.FriendService', 'GetApplications', body);
    return types.GetApplicationsReply.decode(replyBody);
}

async function acceptFriends(gids) {
    const body = types.AcceptFriendsRequest.encode(types.AcceptFriendsRequest.create({
        friend_gids: gids.map(g => toLong(g)),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.friendpb.FriendService', 'AcceptFriends', body);
    return types.AcceptFriendsReply.decode(replyBody);
}

async function deleteFriend(friendGid) {
    const body = types.DelFriendRequest.encode(types.DelFriendRequest.create({
        friend_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.friendpb.FriendService', 'DelFriend', body);
    return types.DelFriendReply.decode(replyBody || Buffer.alloc(0));
}

async function enterFriendFarm(friendGid, reason = 2) {
    const body = types.VisitEnterRequest.encode(types.VisitEnterRequest.create({
        host_gid: toLong(friendGid),
        reason,  // 2=FRIEND, 3=INTERACT
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.visitpb.VisitService', 'Enter', body);
    return types.VisitEnterReply.decode(replyBody);
}

async function leaveFriendFarm(friendGid) {
    const body = types.VisitLeaveRequest.encode(types.VisitLeaveRequest.create({
        host_gid: toLong(friendGid),
    })).finish();
    try {
        await sendMsgAsync('gamepb.visitpb.VisitService', 'Leave', body);
    } catch (e) {
}
}

function getLocalDateKey() {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
}

function normalizeRuntimeText(value) {
    return sanitizeLogText(value);
}

/**
 * Friend farm patrol loop and interactions.
 */
function checkDailyReset() {
    const today = getLocalDateKey();  // YYYY-MM-DD
    if (lastResetDate !== today) {
        if (lastResetDate !== '') {
            log('CONFIG', '已完成每日重置：次数限制与经验追踪已清空');
        }
        operationLimits.clear();
        expExhausted.clear();
        expTracker.clear();
        lastResetDate = today;
    }
}

/**

 */
function maybeAutoEnableDailyFriendActions() {
    const now = new Date();
    const today = getLocalDateKey();
    if (lastDailyActionAutoEnableDate === today) return;

    const nowMinutes = now.getHours() * 60 + now.getMinutes();
    const triggerAtMinutes = 5; // 00:05
    // run once per day within 00:05-00:59 local time
    if (nowMinutes < triggerAtMinutes || nowMinutes >= 60) return;

    const toBool = (value, fallback) => {
        if (value === undefined || value === null || value === '') return fallback;
        const text = String(value).trim().toLowerCase();
        if (['1', 'true', 'yes', 'on'].includes(text)) return true;
        if (['0', 'false', 'no', 'off'].includes(text)) return false;
        return fallback;
    };

    const cfg = readWebuiConfig() || {};
    const legacyCare = toBool(cfg.friendActionWater, true)
        || toBool(cfg.friendActionWeed, true)
        || toBool(cfg.friendActionBug, true);
    const careBefore = toBool(cfg.friendActionCare, legacyCare);
    const prankBefore = toBool(cfg.friendActionPrank, false);

    const updated = writeWebuiConfigPatch({
        friendActionCare: true,
        friendActionPrank: true,
    });
    if (!updated) return;

    friendActionCare = true;
    friendActionPrank = true;
    lastDailyActionAutoEnableDate = today;
    log('CONFIG', `00:05 每日自动开启：照顾 ${careBefore ? '开' : '关'}->开，捣乱 ${prankBefore ? '开' : '关'}->开`);
}

function updateOperationLimits(limits) {
    if (!limits || limits.length === 0) return;
    checkDailyReset();
    for (const limit of limits) {
        const id = toNum(limit.id);
        if (id > 0) {
            const newExpTimes = toNum(limit.day_exp_times);
            const data = {
                dayTimes: toNum(limit.day_times),
                dayTimesLimit: toNum(limit.day_times_lt),
                dayExpTimes: newExpTimes,
                dayExpTimesLimit: toNum(limit.day_ex_times_lt),
            };
            operationLimits.set(id, data);


            if (expTracker.has(id)) {
                const prevExpTimes = expTracker.get(id);
                expTracker.delete(id);
                if (newExpTimes <= prevExpTimes && !expExhausted.has(id)) {

                    expExhausted.add(id);
                    const name = OP_NAMES[id] || `#${id}`;
                    log('FRIEND_EXP', `${name} 经验次数已用尽（dayExpTimes=${newExpTimes}）`);
                }
            }
        }
    }
    maybeAutoDisableFriendCare();
}

/**


 */
function canGetExp(opId) {
    if (expExhausted.has(opId)) return false;
    const limit = operationLimits.get(opId);
    if (!limit) return true;

    if (limit.dayExpTimesLimit > 0) {
        return limit.dayExpTimes < limit.dayExpTimesLimit;
    }
    return true;
}

/**

 */
function canOperate(opId) {
    const limit = operationLimits.get(opId);
    if (!limit) return true;
    if (limit.dayTimesLimit <= 0) return true;
    return limit.dayTimes < limit.dayTimesLimit;
}

/**

 */
function markExpCheck(opId) {
    const limit = operationLimits.get(opId);
    if (limit) {
        expTracker.set(opId, limit.dayExpTimes);
    }
}

/**

 */
function canOperate(opId) {
    const limit = operationLimits.get(opId);
    if (!limit) return true;
    if (limit.dayTimesLimit <= 0) return true;
    return limit.dayTimes < limit.dayTimesLimit;
}

/**

 */
function getRemainingTimes(opId) {
    const limit = operationLimits.get(opId);
    if (!limit || limit.dayTimesLimit <= 0) return 999;
    return Math.max(0, limit.dayTimesLimit - limit.dayTimes);
}

/**

 */
function getOperationLimitsSummary() {
    const parts = [];

    for (const id of [10005, 10006, 10007, 10008]) {
        const limit = operationLimits.get(id);
        if (limit && limit.dayExpTimesLimit > 0) {
            const name = OP_NAMES[id] || `#${id}`;
            const expLeft = limit.dayExpTimesLimit - limit.dayExpTimes;
            parts.push(`${name}${expLeft}/${limit.dayExpTimesLimit}`);
        }
    }

    for (const id of [10003, 10004]) {
        const limit = operationLimits.get(id);
        if (limit && limit.dayTimesLimit > 0) {
            const name = OP_NAMES[id] || `#${id}`;
            const left = limit.dayTimesLimit - limit.dayTimes;
            parts.push(`${name}${left}/${limit.dayTimesLimit}`);
        }
    }
    return parts;
}

async function helpWater(friendGid, landIds) {
    const body = types.WaterLandRequest.encode(types.WaterLandRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'WaterLand', body);
    const reply = types.WaterLandReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}

async function helpWeed(friendGid, landIds) {
    const body = types.WeedOutRequest.encode(types.WeedOutRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'WeedOut', body);
    const reply = types.WeedOutReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}

async function helpInsecticide(friendGid, landIds) {
    const body = types.InsecticideRequest.encode(types.InsecticideRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Insecticide', body);
    const reply = types.InsecticideReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}

async function stealHarvest(friendGid, landIds) {
    const body = types.HarvestRequest.encode(types.HarvestRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
        is_all: true,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Harvest', body);
    const reply = types.HarvestReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}

async function putInsects(friendGid, landIds) {
    const body = types.PutInsectsRequest.encode(types.PutInsectsRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'PutInsects', body);
    const reply = types.PutInsectsReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}

async function putWeeds(friendGid, landIds) {
    const body = types.PutWeedsRequest.encode(types.PutWeedsRequest.create({
        land_ids: landIds,
        host_gid: toLong(friendGid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'PutWeeds', body);
    const reply = types.PutWeedsReply.decode(replyBody);
    updateOperationLimits(reply.operation_limits);
    return reply;
}




const DEBUG_FRIEND_LANDS = false;

function analyzeFriendLands(lands, myGid, friendName = '') {
    const result = {
        stealable: [],
        stealableInfo: [],
        needWater: [],
        needWeed: [],
        needBug: [],
        canPutWeed: [],
        canPutBug: [],
    };

    for (const land of lands) {
        const id = toNum(land.id);
        const plant = land.plant;

        const showDebug = DEBUG_FRIEND_LANDS === true || DEBUG_FRIEND_LANDS === friendName;

        if (!plant || !plant.phases || plant.phases.length === 0) {
            if (showDebug) console.log(`  [${friendName}] land ${id}: no phase data`);
            continue;
        }

        const currentPhase = getCurrentPhase(plant.phases, showDebug, `[${friendName}] land ${id}`);
        if (!currentPhase) {
            if (showDebug) console.log(`  [${friendName}] land ${id}: getCurrentPhase returned null`);
            continue;
        }
        const phaseVal = currentPhase.phase;

        if (showDebug) {
            const insectOwners = plant.insect_owners || [];
            const weedOwners = plant.weed_owners || [];
            console.log('  [' + friendName + '] land ' + id + ': phase=' + phaseVal + ' stealable=' + plant.stealable + ' dry=' + toNum(plant.dry_num) + ' weed=' + weedOwners.length + ' bug=' + insectOwners.length);
        }

        if (phaseVal === PlantPhase.MATURE) {
            if (plant.stealable) {
                result.stealable.push(id);
                const plantId = toNum(plant.id);
                const plantName = getPlantName(plantId) || plant.name || 'unknown';
                const levelNeed = getPlantStealLevelNeed(plantId);
                result.stealableInfo.push({ landId: id, plantId, name: plantName, levelNeed });
            } else if (showDebug) {
                console.log('  [' + friendName + '] mature but not stealable: land ' + id);
            }
            continue;
        }

        if (phaseVal === PlantPhase.DEAD) continue;


        if (toNum(plant.dry_num) > 0) result.needWater.push(id);
        if (plant.weed_owners && plant.weed_owners.length > 0) result.needWeed.push(id);
        if (plant.insect_owners && plant.insect_owners.length > 0) result.needBug.push(id);



        const weedOwners = plant.weed_owners || [];
        const insectOwners = plant.insect_owners || [];
        const iAlreadyPutWeed = weedOwners.some(gid => toNum(gid) === myGid);
        const iAlreadyPutBug = insectOwners.some(gid => toNum(gid) === myGid);


        if (weedOwners.length < 2 && !iAlreadyPutWeed) {
            result.canPutWeed.push(id);
        }
        if (insectOwners.length < 2 && !iAlreadyPutBug) {
            result.canPutBug.push(id);
        }
    }
    return result;
}



async function visitFriend(friend, totalActions, myGid) {
    const { gid, name } = friend;
    const safeName = normalizeRuntimeText(name).slice(0, 48) || `GID:${gid}`;
    const showDebug = DEBUG_FRIEND_LANDS === true || DEBUG_FRIEND_LANDS === name;
    const perActionDelayMs = Math.max(0, Math.floor(Number(friendActionDelayMs) || 0));
    const waitAction = async () => {
        if (perActionDelayMs > 0) await sleep(perActionDelayMs);
    };

    if (showDebug) {
        console.log('\\n========== friend debug start: ' + safeName + ' ==========');
    }

    let enterReply;
    try {
        const enterReason = 2;
        if (friendActionPrank && ENABLE_PUT_BAD_THINGS) {
            log('PRANK', `进入好友农场准备捣乱：${safeName}，reason=${enterReason}`);
        }
        enterReply = await enterFriendFarm(gid, enterReason);
    } catch (e) {
        const errText = normalizeRuntimeText(e && e.message ? e.message : String(e));
        recordFriendVisitFailure(friend, errText);
        logWarn('FRIEND', `进入 ${safeName} 农场失败：${errText}`);
        return;
    }

    const lands = enterReply.lands || [];
    if (showDebug) {
        console.log(`  [${safeName}] loaded friend lands count=${lands.length}`);
    }
    if (lands.length === 0) {
        recordFriendActionStats(friend, {
            steal: 0,
            stealPlants: [],
            careWater: 0,
            careWeed: 0,
            careBug: 0,
            prankBug: 0,
            prankWeed: 0,
        }, true);
        await leaveFriendFarm(gid);
        return;
    }

    const status = analyzeFriendLands(lands, myGid, name);
    const prankDiagEnabled = friendActionPrank && ENABLE_PUT_BAD_THINGS;
    const prankBugRemainingBefore = prankDiagEnabled && canOperate(10004) ? getRemainingTimes(10004) : 0;
    const prankWeedRemainingBefore = prankDiagEnabled && canOperate(10003) ? getRemainingTimes(10003) : 0;
    let prankBugAttempted = 0;
    let prankBugFail = 0;
    let prankBugFirstErr = '';
    let prankWeedAttempted = 0;
    let prankWeedFail = 0;
    let prankWeedFirstErr = '';
    let prankDisabledByFailure = false;
    
    if (showDebug) {
        console.log(`  [${name}] land summary: stealable=${status.stealable.length} water=${status.needWater.length} weed=${status.needWeed.length} bug=${status.needBug.length}`);
        console.log('========== friend debug ready: ' + name + ' ==========');
    }


    if (prankDiagEnabled) {
        log('PRANK', `捣乱开始：${name}，剩余放虫=${prankBugRemainingBefore}，剩余放草=${prankWeedRemainingBefore}`);
    }

    const actionDetail = {
        steal: 0,
        stealPlants: [],
        careWater: 0,
        careWeed: 0,
        careBug: 0,
        prankBug: 0,
        prankWeed: 0,
    };


    const stealEnabledForFriend = friend.stealEnabled !== false && isFriendStealEnabled(friend);
    if (friendActionSteal && stealEnabledForFriend && status.stealable.length > 0) {
        const stealTargets = [];
        for (let i = 0; i < status.stealable.length; i++) {
            const landId = status.stealable[i];
            const info = status.stealableInfo[i] || null;
            const levelNeed = Number(info && info.levelNeed || 0);
            if (friendStealLevelThreshold > 0 && !(levelNeed > friendStealLevelThreshold)) {
                continue;
            }
            stealTargets.push({ landId, info });
        }

        let ok = 0;
        const stolenPlants = [];
        for (const target of stealTargets) {
            try {
                await stealHarvest(gid, [target.landId]);
                ok++;
                if (target.info && target.info.name) {
                    stolenPlants.push(normalizeRuntimeText(target.info.name).slice(0, 24));
                }
            } catch (e) { /* ignore */ }
            await waitAction();
        }
        if (ok > 0) {
            actionDetail.steal += ok;
            actionDetail.stealPlants.push(...stolenPlants);
            totalActions.steal += ok;
        }
    }


    if (friendActionCare && status.needWeed.length > 0) {
        const shouldHelp = !HELP_ONLY_WITH_EXP || canGetExp(10005);
        if (shouldHelp) {
            markExpCheck(10005);
            let ok = 0;
            for (const landId of status.needWeed) {
                try { await helpWeed(gid, [landId]); ok++; } catch (e) { /* ignore */ }
                await waitAction();
            }
            if (ok > 0) {
                actionDetail.careWeed += ok;
                totalActions.weed += ok;
            }
        }
    }

    if (friendActionCare && status.needBug.length > 0) {
        const shouldHelp = !HELP_ONLY_WITH_EXP || canGetExp(10006);
        if (shouldHelp) {
            markExpCheck(10006);
            let ok = 0;
            for (const landId of status.needBug) {
                try { await helpInsecticide(gid, [landId]); ok++; } catch (e) { /* ignore */ }
                await waitAction();
            }
            if (ok > 0) {
                actionDetail.careBug += ok;
                totalActions.bug += ok;
            }
        }
    }

    if (friendActionCare && status.needWater.length > 0) {
        const shouldHelp = !HELP_ONLY_WITH_EXP || canGetExp(10007);
        if (shouldHelp) {
            markExpCheck(10007);
            let ok = 0;
            for (const landId of status.needWater) {
                try { await helpWater(gid, [landId]); ok++; } catch (e) { /* ignore */ }
                await waitAction();
            }
            if (ok > 0) {
                actionDetail.careWater += ok;
                totalActions.water += ok;
            }
        }
    }


    if (friendActionPrank && ENABLE_PUT_BAD_THINGS && status.canPutBug.length > 0 && canOperate(10004)) {
        let ok = 0;
        const remaining = getRemainingTimes(10004);
        const toProcess = status.canPutBug.slice(0, remaining);
        for (const landId of toProcess) {
            if (!canOperate(10004)) break;
            prankBugAttempted++;
            try {
                await putInsects(gid, [landId]);
                ok++;
            } catch (e) {
                prankBugFail++;
                if (!prankBugFirstErr) prankBugFirstErr = e && e.message ? e.message : String(e);
                prankDisabledByFailure = maybeAutoDisableFriendPrankOnFailure('putInsects_failed', prankBugFirstErr) || prankDisabledByFailure;
                break;
            }
            await waitAction();
        }
        if (ok > 0) {
            actionDetail.prankBug += ok;
            totalActions.putBug += ok;
        }
    }

    if (friendActionPrank && ENABLE_PUT_BAD_THINGS && status.canPutWeed.length > 0 && canOperate(10003)) {
        let ok = 0;
        const remaining = getRemainingTimes(10003);
        const toProcess = status.canPutWeed.slice(0, remaining);
        for (const landId of toProcess) {
            if (!canOperate(10003)) break;
            prankWeedAttempted++;
            try {
                await putWeeds(gid, [landId]);
                ok++;
            } catch (e) {
                prankWeedFail++;
                if (!prankWeedFirstErr) prankWeedFirstErr = e && e.message ? e.message : String(e);
                prankDisabledByFailure = maybeAutoDisableFriendPrankOnFailure('putWeeds_failed', prankWeedFirstErr) || prankDisabledByFailure;
                break;
            }
            await waitAction();
        }
        if (ok > 0) {
            actionDetail.prankWeed += ok;
            totalActions.putWeed += ok;
        }
    }
    if (prankDiagEnabled) {
        const bugSuccess = prankBugAttempted - prankBugFail;
        const weedSuccess = prankWeedAttempted - prankWeedFail;
        const bugRemainingAfter = canOperate(10004) ? getRemainingTimes(10004) : 0;
        const weedRemainingAfter = canOperate(10003) ? getRemainingTimes(10003) : 0;
        const shouldLogPrankDiag =
            prankBugAttempted > 0
            || prankWeedAttempted > 0
            || status.canPutBug.length > 0
            || status.canPutWeed.length > 0
            || (!friend.hasSteal && !friend.hasHelp);

        if (shouldLogPrankDiag) {
            let reasonText = 'no prank action quota remaining';
            if (prankBugAttempted === 0 && prankWeedAttempted === 0) {
                if (status.canPutBug.length === 0 && status.canPutWeed.length === 0) {
                    reasonText = 'no prank action quota remaining';
                } else if (prankBugRemainingBefore <= 0 && prankWeedRemainingBefore <= 0) {
                    reasonText = 'no prank action quota remaining';
                }
            }

            log('PRANK', '本轮捣乱诊断完成');
        }
    }

    const actionSummary = [];
    const stealCount = Math.max(0, Math.floor(Number(actionDetail.steal) || 0));
    if (stealCount > 0) {
        const plantNames = [...new Set(actionDetail.stealPlants)]
            .map((name) => normalizeRuntimeText(name).slice(0, 24))
            .filter(Boolean)
            .slice(0, 8)
            .join('/');
        actionSummary.push(`偷菜${stealCount}${plantNames ? `(${plantNames})` : ''}`);
    }

    const careWaterCount = Math.max(0, Math.floor(Number(actionDetail.careWater) || 0));
    const careWeedCount = Math.max(0, Math.floor(Number(actionDetail.careWeed) || 0));
    const careBugCount = Math.max(0, Math.floor(Number(actionDetail.careBug) || 0));
    const careTotal = careWaterCount + careWeedCount + careBugCount;
    if (careTotal > 0) {
        const careParts = [];
        if (careWaterCount > 0) careParts.push(`浇水${careWaterCount}`);
        if (careWeedCount > 0) careParts.push(`除草${careWeedCount}`);
        if (careBugCount > 0) careParts.push(`除虫${careBugCount}`);
        actionSummary.push(`照顾${careTotal}${careParts.length ? `(${careParts.join('/')})` : ''}`);
    }

    const prankBugCount = Math.max(0, Math.floor(Number(actionDetail.prankBug) || 0));
    const prankWeedCount = Math.max(0, Math.floor(Number(actionDetail.prankWeed) || 0));
    const prankTotal = prankBugCount + prankWeedCount;
    if (prankTotal > 0) {
        const prankParts = [];
        if (prankBugCount > 0) prankParts.push(`放虫${prankBugCount}`);
        if (prankWeedCount > 0) prankParts.push(`放草${prankWeedCount}`);
        actionSummary.push(`捣乱${prankTotal}${prankParts.length ? `(${prankParts.join('/')})` : ''}`);
    }

    if (actionSummary.length > 0) {
        const safeName = normalizeRuntimeText(name).slice(0, 48) || `GID:${gid}`;
        const safeSummary = normalizeRuntimeText(actionSummary.join(' | ')).slice(0, 220);
        log('FRIEND', `${safeName}: ${safeSummary}`);
    }

    recordFriendActionStats(friend, actionDetail, true);

    await leaveFriendFarm(gid);
}



async function checkFriends() {
    const state = getUserState();
    if (isCheckingFriends || !state.gid) return false;
    isCheckingFriends = true;
    let checkTimedOut = false;

    // Reset daily action flags when day changes.
    checkDailyReset();

    try {
        const friendsReply = await getAllFriends();
        const friends = friendsReply.game_friends || [];
        friendLoopTimeoutBackoffMs = 0;

        writeWebuiFriendsSnapshot(friends, toNum(state.gid));
        syncFriendStatsFromOfficialList(friends, toNum(state.gid));
        maybeQueueAutoDeleteNoStealFriends(friends, toNum(state.gid));
        if (friends.length === 0) {
            friendNoActionStreak += 1;
            log('FRIEND', '好友列表为空，本轮跳过');
            return false;
        }

        const helpFeatureEnabled = friendActionCare;
        const canHelpWithExp = helpFeatureEnabled
            && (!HELP_ONLY_WITH_EXP || canGetExp(10005) || canGetExp(10006) || canGetExp(10007));
        const betweenFriendDelayMs = Math.max(0, Math.floor(Number(friendBetweenFriendDelayMs) || 0));
        const canPutBugOrWeed = canOperate(10004) || canOperate(10003);

        // Always process steal first, then help, then prank-only targets.
        const stealFriends = [];
        const helpFriends = [];
        const otherFriends = [];
        const prankDiagEnabled = friendActionPrank && ENABLE_PUT_BAD_THINGS;
        const prankBugRemainAtCycleStart = canOperate(10004) ? getRemainingTimes(10004) : 0;
        const prankWeedRemainAtCycleStart = canOperate(10003) ? getRemainingTimes(10003) : 0;
        if (prankDiagEnabled) {
            log('PRANK', `本轮开始：可放虫=${prankBugRemainAtCycleStart}，可放草=${prankWeedRemainAtCycleStart}，开关=${canPutBugOrWeed ? '开' : '关'}`);
        }

        const visitedGids = new Set();
        for (const f of friends) {
            const gid = toNum(f.gid);
            if (gid === state.gid || visitedGids.has(gid)) continue;

            const name = normalizeRuntimeText(f.remark || f.name || `GID:${gid}`).slice(0, 48) || `GID:${gid}`;
            const stealEnabled = isFriendStealEnabled({ gid, name });
            const p = f.plant;

            const stealNum = p ? toNum(p.steal_plant_num) : 0;
            const dryNum = p ? toNum(p.dry_num) : 0;
            const weedNum = p ? toNum(p.weed_num) : 0;
            const insectNum = p ? toNum(p.insect_num) : 0;

            const hasSteal = friendActionSteal && stealEnabled && stealNum > 0;
            const hasHelp = friendActionCare && (dryNum > 0 || weedNum > 0 || insectNum > 0);

            const showDebug = DEBUG_FRIEND_LANDS === true || DEBUG_FRIEND_LANDS === name;
            if (showDebug) {
                console.log(`[DEBUG] friend preview [${name}]: steal=${stealNum} dry=${dryNum} weed=${weedNum} insect=${insectNum}`);
            }

            if (hasSteal) {
                stealFriends.push({ gid, name, level: toNum(f.level), hasSteal: true, hasHelp, stealEnabled });
                visitedGids.add(gid);
            } else if (hasHelp && canHelpWithExp) {
                helpFriends.push({ gid, name, level: toNum(f.level), hasSteal: false, hasHelp: true, stealEnabled });
                visitedGids.add(gid);
            } else if (friendActionPrank && ENABLE_PUT_BAD_THINGS && canPutBugOrWeed) {
                otherFriends.push({ gid, name, level: toNum(f.level), hasSteal: false, hasHelp: false, stealEnabled });
                visitedGids.add(gid);
            }

            if (showDebug && visitedGids.has(gid)) {
                const pos = stealFriends.length + helpFriends.length + otherFriends.length;
                console.log(`[DEBUG] friend [${name}] queued at ${pos}`);
            }
        }

        const friendsToVisit = [...stealFriends, ...helpFriends, ...otherFriends];
        if (prankDiagEnabled) {
            log('PRANK', `队列统计：偷菜=${stealFriends.length}，照顾=${helpFriends.length}，仅捣乱=${otherFriends.length}，合计=${friendsToVisit.length}`);
        }

        if (DEBUG_FRIEND_LANDS && typeof DEBUG_FRIEND_LANDS === 'string') {
            const idx = friendsToVisit.findIndex((f) => f.name === DEBUG_FRIEND_LANDS);
            if (idx >= 0) {
                const inSteal = idx < stealFriends.length;
                const inHelp = !inSteal && idx < (stealFriends.length + helpFriends.length);
                const group = inSteal ? 'steal' : (inHelp ? 'help' : 'other');
                console.log(`[DEBUG] friend [${DEBUG_FRIEND_LANDS}] index ${idx + 1}/${friendsToVisit.length} (${group})`);
            } else {
                console.log(`[DEBUG] friend [${DEBUG_FRIEND_LANDS}] not in visit list`);
            }
        }

        const totalFriendCount = friends.length;
        const queuedFriendCount = friendsToVisit.length;

        if (queuedFriendCount === 0) {
            friendNoActionStreak += 1;
            if (prankDiagEnabled) log('PRANK', '本轮结束：无可访问好友');
            return false;
        }

        const totalActions = { steal: 0, water: 0, weed: 0, bug: 0, putBug: 0, putWeed: 0 };
        for (let i = 0; i < friendsToVisit.length; i++) {
            const friend = friendsToVisit[i];
            if (!friendActionPrank && !friend.hasSteal && !friend.hasHelp) continue;

            const showDebug = DEBUG_FRIEND_LANDS === true || DEBUG_FRIEND_LANDS === friend.name;
            if (showDebug) {
                console.log(`[DEBUG] visiting [${friend.name}] (${i + 1}/${friendsToVisit.length})`);
            }

            try {
                await visitFriend(friend, totalActions, state.gid);
            } catch (e) {
                if (showDebug) console.log(`[DEBUG] visit error [${friend.name}]: ${e.message}`);
            }

            if (betweenFriendDelayMs > 0) {
                await sleep(betweenFriendDelayMs);
            }
        }

        const summary = [];
        if (totalActions.steal > 0) summary.push(`偷菜${totalActions.steal}`);
        const careTotal = totalActions.weed + totalActions.bug + totalActions.water;
        if (careTotal > 0) {
            const careParts = [];
            if (totalActions.water > 0) careParts.push(`浇水${totalActions.water}`);
            if (totalActions.weed > 0) careParts.push(`除草${totalActions.weed}`);
            if (totalActions.bug > 0) careParts.push(`除虫${totalActions.bug}`);
            summary.push(`照顾${careTotal}${careParts.length ? `(${careParts.join('/')})` : ''}`);
        }

        const prankTotal = totalActions.putBug + totalActions.putWeed;
        if (prankTotal > 0) {
            const prankParts = [];
            if (totalActions.putBug > 0) prankParts.push(`放虫${totalActions.putBug}`);
            if (totalActions.putWeed > 0) prankParts.push(`放草${totalActions.putWeed}`);
            summary.push(`捣乱${prankTotal}${prankParts.length ? `(${prankParts.join('/')})` : ''}`);
        }

        if (summary.length > 0) {
            friendNoActionStreak = 0;
            log('FRIEND', `巡查队列${queuedFriendCount}/${totalFriendCount}位好友：${summary.join('，')}`);
        } else {
            friendNoActionStreak += 1;
        }
        if (prankDiagEnabled) {
            const prankBugRemainAtCycleEnd = canOperate(10004) ? getRemainingTimes(10004) : 0;
            const prankWeedRemainAtCycleEnd = canOperate(10003) ? getRemainingTimes(10003) : 0;
            const prankSuccessTotal = totalActions.putBug + totalActions.putWeed;
            if (prankSuccessTotal > 0) {
                log('PRANK', `本轮结束：捣乱${prankSuccessTotal}（放虫${totalActions.putBug}/放草${totalActions.putWeed}），剩余放虫=${prankBugRemainAtCycleEnd}，剩余放草=${prankWeedRemainAtCycleEnd}`);
            } else {
                log('PRANK', `本轮结束：未执行捣乱，剩余放虫=${prankBugRemainAtCycleEnd}，剩余放草=${prankWeedRemainAtCycleEnd}`);
            }
        }

        isFirstFriendCheck = false;
        friendLoopTimeoutBackoffMs = 0;
    } catch (err) {
        friendNoActionStreak = 0;
        const msg = normalizeRuntimeText((err && err.message) || err || '');
        checkTimedOut = /请求超时|request timeout|timeout/i.test(msg);
        if (checkTimedOut) {
            friendLoopTimeoutBackoffMs = friendLoopTimeoutBackoffMs > 0
                ? Math.min(friendLoopTimeoutBackoffMs * 2, LOOP_TIMEOUT_BACKOFF_MAX_MS)
                : LOOP_TIMEOUT_BACKOFF_BASE_MS;
            logWarn('FRIEND', `巡查失败：${msg}（临时退避 ${friendLoopTimeoutBackoffMs}ms）`);
        } else {
            friendLoopTimeoutBackoffMs = 0;
            logWarn('FRIEND', `巡查失败：${msg}`);
        }
    } finally {
        flushFriendStatsIfDirty();
        isCheckingFriends = false;
    }
    return checkTimedOut;
}

/**


 */
function isInFriendActiveWindow(start, end) {
    return isInTimeWindow(start, end, friendActiveAllDay);
}

function isInTimeWindow(start, end, allDayFlag = false) {
    if (allDayFlag) return true;
    if (!start || !end) return false;
    const now = new Date();
    const nowMin = now.getHours() * 60 + now.getMinutes();
    const [sh, sm] = start.split(':').map(Number);
    const [eh, em] = end.split(':').map(Number);
    const startMin = sh * 60 + sm;
    const endMin = eh * 60 + em;
    if (startMin === endMin) return true;
    if (startMin <= endMin) {
        return nowMin >= startMin && nowMin < endMin;
    }

    return nowMin >= startMin || nowMin < endMin;
}

/**

 */
function normalizeIntervalSec(value, fallbackSec) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallbackSec;
    return Math.min(Math.max(Math.floor(n), 0), 300);
}

function normalizePerformanceMode(value) {
    const key = String(value || '').trim().toLowerCase();
    if (Object.prototype.hasOwnProperty.call(FRIEND_PERFORMANCE_PROFILES, key)) return key;
    return 'standard';
}

function getPerformanceProfile(mode) {
    const key = normalizePerformanceMode(mode);
    return FRIEND_PERFORMANCE_PROFILES[key] || FRIEND_PERFORMANCE_PROFILES.standard;
}

function readWebuiConfig() {
    try {
        const stat = fs.statSync(WEBUI_CONFIG_PATH);
        if (stat.mtimeMs === webuiConfigMtimeMs) return webuiConfigCache;

        const parsed = JSON.parse(fs.readFileSync(WEBUI_CONFIG_PATH, 'utf8')) || {};
        webuiConfigMtimeMs = stat.mtimeMs;
        webuiConfigCache = typeof parsed === 'object' && parsed ? parsed : {};
        return webuiConfigCache;
    } catch (err) {
        webuiConfigMtimeMs = -1;
        webuiConfigCache = null;
        return null;
    }
}

function writeWebuiConfigPatch(patch = {}) {
    try {
        const current = readWebuiConfig() || {};
        const next = { ...current, ...patch };
        const dir = path.dirname(WEBUI_CONFIG_PATH);
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(WEBUI_CONFIG_PATH, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
        try {
            const stat = fs.statSync(WEBUI_CONFIG_PATH);
            webuiConfigMtimeMs = stat.mtimeMs;
        } catch (e) {
            webuiConfigMtimeMs = -1;
        }
        webuiConfigCache = next;
        return next;
    } catch (err) {
        logWarn('CONFIG', `write webui config failed: ${normalizeRuntimeText(err.message)}`);
        return null;
    }
}

function isCareExpFullyExhausted() {
    return !canGetExp(10005) && !canGetExp(10006) && !canGetExp(10007);
}

function maybeAutoDisableFriendCare() {
    if (!HELP_ONLY_WITH_EXP) return;
    if (!friendActionCare) return;
    if (!isCareExpFullyExhausted()) return;

    const updated = writeWebuiConfigPatch({ friendActionCare: false });
    if (!updated) return;

    friendActionCare = false;
    log('CONFIG', '照顾已自动关闭（经验次数已用尽）');
}

function maybeAutoDisableFriendPrankOnFailure(reason = '', errMsg = '') {
    if (!friendActionPrank) return false;

    const updated = writeWebuiConfigPatch({ friendActionPrank: false });
    friendActionPrank = false;
    if (!updated) {
        logWarn('CONFIG', '自动关闭捣乱写配置失败，仅本次运行已关闭');
    }
    const detail = [
        reason ? `reason=${reason}` : '',
        errMsg ? `error=${String(errMsg).replace(/\s+/g, ' ').slice(0, 180)}` : '',
    ].filter(Boolean).join(' ');
    log('CONFIG', `捣乱已自动关闭（执行失败）${detail ? ` ${detail}` : ''}`);
    return true;
}

function applyRuntimeFriendConfig() {
    const cfg = readWebuiConfig();
    if (!cfg) return;
    const toBool = (value, fallback) => {
        if (value === undefined || value === null || value === '') return fallback;
        const text = String(value).trim().toLowerCase();
        if (['1', 'true', 'yes', 'on'].includes(text)) return true;
        if (['0', 'false', 'no', 'off'].includes(text)) return false;
        return fallback;
    };

    const nextPerformanceMode = normalizePerformanceMode(cfg.performanceMode);
    const performanceProfile = getPerformanceProfile(nextPerformanceMode);
    const profileIntervalFallbackSec = normalizeIntervalSec(performanceProfile.friendIntervalSec, 1);
    const nextSec = normalizeIntervalSec(cfg.friendIntervalSec, profileIntervalFallbackSec);
    const nextMs = nextSec * 1000;
    if (nextMs !== CONFIG.friendCheckInterval) {
        CONFIG.friendCheckInterval = nextMs;
        log('CONFIG', `好友巡查间隔热更新为 ${nextSec}s`);
    }

    const normalizeHHMM = (v) => (/^\d{2}:\d{2}$/.test(String(v || '').trim()) ? String(v).trim() : '');
    const nextFriendActiveStart = normalizeHHMM(cfg.friendActiveStart);
    const nextFriendActiveEnd = normalizeHHMM(cfg.friendActiveEnd);
    const nextFriendActiveAllDay = toBool(cfg.friendActiveAllDay, false);
    const nextFriendApplyActiveStart = normalizeHHMM(cfg.friendApplyActiveStart);
    const nextFriendApplyActiveEnd = normalizeHHMM(cfg.friendApplyActiveEnd);
    const nextFriendApplyAllDay = toBool(cfg.friendApplyAllDay, true);
    const nextFriendActionSteal = toBool(cfg.friendActionSteal, true);
    const legacyCare = toBool(cfg.friendActionWater, true)
        || toBool(cfg.friendActionWeed, true)
        || toBool(cfg.friendActionBug, true);
    const nextFriendActionCare = toBool(cfg.friendActionCare, legacyCare);
    const nextFriendActionPrank = toBool(cfg.friendActionPrank, false);
    const nextFriendStealEnabledMap = normalizeFriendStealEnabled(cfg.friendStealEnabled);
    const nextFriendStealEnabledSig = JSON.stringify(nextFriendStealEnabledMap);
    const nextStealLevelThreshold = Math.max(0, Math.floor(Number(cfg.stealLevelThreshold) || 0));
    const nextFriendAutoDeleteNoStealEnabled = toBool(cfg.friendAutoDeleteNoStealEnabled, false);
    const nextFriendAutoDeleteNoStealDays = Math.max(1, Math.floor(Number(cfg.friendAutoDeleteNoStealDays) || 7));
    const nextFriendActionDelayMs = Math.max(0, Math.floor(Number(performanceProfile.actionDelayMs) || 0));
    const nextFriendBetweenFriendDelayMs = Math.max(0, Math.floor(Number(performanceProfile.betweenFriendDelayMs) || 0));
    const staleDeleteRequestIds = [];
    const activeDeleteRequests = [];
    for (const item of normalizeFriendDeleteRequests(cfg.friendDeleteRequests)) {
        if (shouldIgnoreStaleFriendRequest(item.requestedAtMs)) {
            const ageSec = Math.max(0, Math.floor((Date.now() - item.requestedAtMs) / 1000));
            log('CONFIG', `ignore stale friend-delete request gid=${item.gid} (${ageSec}s old)`);
            staleDeleteRequestIds.push(item.requestId);
            continue;
        }
        activeDeleteRequests.push(item);
    }

    if (
        friendActiveStart !== nextFriendActiveStart
        || friendActiveEnd !== nextFriendActiveEnd
        || friendActiveAllDay !== nextFriendActiveAllDay
    ) {
        const nextWindowText = nextFriendActiveAllDay
            ? 'all-day'
            : (nextFriendActiveStart && nextFriendActiveEnd ? `${nextFriendActiveStart}-${nextFriendActiveEnd}` : 'disabled');
        log('CONFIG', `好友巡查时段热更新为 ${nextWindowText}`);
    }
    if (
        friendApplyActiveStart !== nextFriendApplyActiveStart
        || friendApplyActiveEnd !== nextFriendApplyActiveEnd
        || friendApplyAllDay !== nextFriendApplyAllDay
    ) {
        const nextApplyWindowText = nextFriendApplyAllDay
            ? 'all-day'
            : (nextFriendApplyActiveStart && nextFriendApplyActiveEnd ? `${nextFriendApplyActiveStart}-${nextFriendApplyActiveEnd}` : 'disabled');
        log('CONFIG', `好友申请处理时段热更新为 ${nextApplyWindowText}`);
    }
    if (friendActionSteal !== nextFriendActionSteal) {
        log('CONFIG', `偷菜开关热更新为 ${nextFriendActionSteal ? '开' : '关'}`);
    }
    if (friendActionCare !== nextFriendActionCare) {
        log('CONFIG', `照顾开关热更新为 ${nextFriendActionCare ? '开' : '关'}`);
    }
    if (friendActionPrank !== nextFriendActionPrank) {
        log('CONFIG', `捣乱开关热更新为 ${nextFriendActionPrank ? '开' : '关'}`);
    }
    if (friendStealLevelThreshold !== nextStealLevelThreshold) {
        log('CONFIG', `偷菜等级阈值热更新为 >${nextStealLevelThreshold}`);
    }
    if (
        friendAutoDeleteNoStealEnabled !== nextFriendAutoDeleteNoStealEnabled
        || friendAutoDeleteNoStealDays !== nextFriendAutoDeleteNoStealDays
    ) {
        log(
            'CONFIG',
            `连续偷不到自动删热更新为 ${nextFriendAutoDeleteNoStealEnabled ? `开(${nextFriendAutoDeleteNoStealDays}天)` : '关'}`
        );
        lastFriendAutoDeleteNoStealSweepAt = 0;
    }
    if (friendStealEnabledMapSig !== nextFriendStealEnabledSig) {
        log('CONFIG', '好友偷菜目标开关已热更新');
    }
    if (
        friendPerformanceMode !== nextPerformanceMode
        || friendActionDelayMs !== nextFriendActionDelayMs
        || friendBetweenFriendDelayMs !== nextFriendBetweenFriendDelayMs
    ) {
        const modeName = nextPerformanceMode === 'retire'
            ? '休养生息'
            : (nextPerformanceMode === 'berserk' ? '不当人喽' : '中规中矩');
        log(
            'CONFIG',
            `好友巡查模式热更新：${modeName}（巡查${nextSec}s / 操作等待${nextFriendActionDelayMs}ms / 好友切换等待${nextFriendBetweenFriendDelayMs}ms）`
        );
        friendNoActionStreak = 0;
    }

    const addedDeleteRequests = enqueueFriendDeleteRequests(activeDeleteRequests);
    if (addedDeleteRequests.length > 0) {
        const names = addedDeleteRequests
            .map((item) => normalizeRuntimeText(item.name || `GID:${item.gid}`).slice(0, 24) || `GID:${item.gid}`)
            .join(', ');
        log('FRIEND_DELETE', `收到 ${addedDeleteRequests.length} 条删好友请求：${names}`);
    }
    if (staleDeleteRequestIds.length > 0) {
        removeFriendDeleteRequestsFromConfig(staleDeleteRequestIds);
    }

    friendActiveStart = nextFriendActiveStart;
    friendActiveEnd = nextFriendActiveEnd;
    friendActiveAllDay = nextFriendActiveAllDay;
    friendApplyActiveStart = nextFriendApplyActiveStart;
    friendApplyActiveEnd = nextFriendApplyActiveEnd;
    friendApplyAllDay = nextFriendApplyAllDay;
    friendActionSteal = nextFriendActionSteal;
    friendActionCare = nextFriendActionCare;
    friendActionPrank = nextFriendActionPrank;
    friendStealEnabledMap = nextFriendStealEnabledMap;
    friendStealEnabledMapSig = nextFriendStealEnabledSig;
    friendStealLevelThreshold = nextStealLevelThreshold;
    friendAutoDeleteNoStealEnabled = nextFriendAutoDeleteNoStealEnabled;
    friendAutoDeleteNoStealDays = nextFriendAutoDeleteNoStealDays;
    friendPerformanceMode = nextPerformanceMode;
    friendActionDelayMs = nextFriendActionDelayMs;
    friendBetweenFriendDelayMs = nextFriendBetweenFriendDelayMs;
}

function getBerserkIdleExtraWaitMs() {
    if (friendPerformanceMode !== 'berserk') return 0;

    if (friendNoActionStreak >= 48) return 1300;
    if (friendNoActionStreak >= 28) return 900;
    if (friendNoActionStreak >= 14) return 520;
    if (friendNoActionStreak >= 6) return 280;
    return 0;
}

async function friendCheckLoop() {
    while (friendLoopRunning) {
        applyRuntimeFriendConfig();
        maybeAutoEnableDailyFriendActions();
        await processPendingFriendDeleteRequests();
        await processPendingFriendApplications();

        if (!isInFriendActiveWindow(friendActiveStart, friendActiveEnd)) {
            if (Date.now() - lastWindowSkipLogAt >= 10 * 60 * 1000) {
                const reason = (!friendActiveStart || !friendActiveEnd)
                    ? '未配置好友巡查时段，跳过本轮'
                    : `当前时间不在好友巡查时段（${friendActiveStart}-${friendActiveEnd}），跳过本轮`;
                log('CONFIG', reason);
                lastWindowSkipLogAt = Date.now();
            }
            if (!friendLoopRunning) break;
            const minLoopWaitMs = friendPerformanceMode === 'berserk' ? BERSERK_MIN_LOOP_WAIT_MS : 0;
            const idleExtraWaitMs = getBerserkIdleExtraWaitMs();
            const networkGuardWaitMs = getFriendNetworkGuardWaitMs();
            const waitMs = Math.max(
                Number(CONFIG.friendCheckInterval) || 0,
                friendLoopTimeoutBackoffMs,
                minLoopWaitMs,
                idleExtraWaitMs,
                networkGuardWaitMs
            );
            if (waitMs > 0) {
                await sleep(waitMs);
            }
            continue;
        }

        lastWindowSkipLogAt = 0;
        await checkFriends();
        if (!friendLoopRunning) break;
        const minLoopWaitMs = friendPerformanceMode === 'berserk' ? BERSERK_MIN_LOOP_WAIT_MS : 0;
        const idleExtraWaitMs = getBerserkIdleExtraWaitMs();
        const networkGuardWaitMs = getFriendNetworkGuardWaitMs();
        const waitMs = Math.max(
            Number(CONFIG.friendCheckInterval) || 0,
            friendLoopTimeoutBackoffMs,
            minLoopWaitMs,
            idleExtraWaitMs,
            networkGuardWaitMs
        );
        if (waitMs > 0) {
            await sleep(waitMs);
        }
    }
}

function startFriendCheckLoop() {
    if (friendLoopRunning) return;
    friendLoopRunning = true;


    setOperationLimitsCallback(updateOperationLimits);


    networkEvents.on('friendApplicationReceived', onFriendApplicationReceived);


    friendCheckTimer = setTimeout(() => friendCheckLoop(), 5000);


    setTimeout(() => checkAndAcceptApplications(), 3000);
}

function stopFriendCheckLoop() {
    friendLoopRunning = false;
    networkEvents.off('friendApplicationReceived', onFriendApplicationReceived);
    if (friendCheckTimer) { clearTimeout(friendCheckTimer); friendCheckTimer = null; }
}



/**

 */
function onFriendApplicationReceived(applications) {
    const rows = Array.isArray(applications) ? applications : [];
    const names = rows.map(a => a.name || `GID:${toNum(a.gid)}`).join(', ');
    if (rows.length > 0) {
        log('FRIEND_APPLY', `收到 ${rows.length} 条好友申请：${names}`);
    }
    enqueueFriendApplications(rows);
    void processPendingFriendApplications();
}

function enqueueFriendApplications(applications) {
    const rows = Array.isArray(applications) ? applications : [];
    for (const app of rows) {
        const gid = toNum(app && app.gid);
        if (!gid) continue;
        const name = String(app && app.name || app && app.remark || `GID:${gid}`).trim() || `GID:${gid}`;
        pendingFriendApplications.set(gid, name);
    }
}

async function processPendingFriendDeleteRequests() {
    if (pendingFriendDeleteRequests.length <= 0) return;
    if (isProcessingPendingFriendDeleteRequests) return;

    isProcessingPendingFriendDeleteRequests = true;
    const completedRequestIds = [];
    let deletedCount = 0;

    try {
        while (pendingFriendDeleteRequests.length > 0) {
            const request = pendingFriendDeleteRequests[0];
            const gid = toNum(request && request.gid);
            const safeName = normalizeRuntimeText(request && request.name ? request.name : `GID:${gid}`).slice(0, 48) || `GID:${gid}`;
            if (gid <= 0) {
                completedRequestIds.push(String(request && request.requestId || '').trim());
                pendingFriendDeleteRequests.shift();
                continue;
            }

            try {
                await deleteFriend(gid);
                pendingFriendApplications.delete(gid);
                pendingFriendDeleteRequests.shift();
                completedRequestIds.push(String(request.requestId || '').trim());
                deletedCount += 1;
                log('FRIEND_DELETE', `已删除好友：${safeName} (GID:${gid})`);
            } catch (e) {
                logWarn('FRIEND_DELETE', `删除好友失败：${safeName} (GID:${gid}) ${normalizeRuntimeText((e && e.message) || String(e))}`);
                break;
            }
        }
    } finally {
        isProcessingPendingFriendDeleteRequests = false;
    }

    if (completedRequestIds.length > 0) {
        removeFriendDeleteRequestsFromConfig(completedRequestIds);
    }

    if (deletedCount > 0) {
        try {
            const state = getUserState();
            const myGid = toNum(state && state.gid);
            const friendsReply = await getAllFriends();
            const friends = Array.isArray(friendsReply && friendsReply.game_friends) ? friendsReply.game_friends : [];
            writeWebuiFriendsSnapshot(friends, myGid);
            syncFriendStatsFromOfficialList(friends, myGid);
            flushFriendStatsIfDirty();
        } catch (e) {
            logWarn('FRIEND_DELETE', `删除后刷新好友快照失败：${normalizeRuntimeText((e && e.message) || String(e))}`);
        }
    }
}

async function processPendingFriendApplications() {
    if (pendingFriendApplications.size <= 0) return;
    if (isProcessingPendingFriendApplications) return;
    isProcessingPendingFriendApplications = true;
    try {
        if (!isInTimeWindow(friendApplyActiveStart, friendApplyActiveEnd, friendApplyAllDay)) {
            if (Date.now() - lastApplyWindowSkipLogAt >= 10 * 60 * 1000) {
                const windowText = friendApplyAllDay
                    ? 'all-day'
                    : (friendApplyActiveStart && friendApplyActiveEnd ? `${friendApplyActiveStart}-${friendApplyActiveEnd}` : 'disabled');
                log('CONFIG', `好友申请处理时段未生效（${windowText}），当前待处理=${pendingFriendApplications.size}`);
                lastApplyWindowSkipLogAt = Date.now();
            }
            return;
        }

        lastApplyWindowSkipLogAt = 0;
        const queuedGids = Array.from(pendingFriendApplications.keys());
        if (queuedGids.length <= 0) return;

        const { currentCount, remainingSlots } = await getFriendCapacityInfo();
        if (remainingSlots <= 0) {
            if (Date.now() - lastFriendApplyFullLogAt >= 10 * 60 * 1000) {
                log('FRIEND_APPLY', `好友位已满（${currentCount}/${FRIEND_MAX_COUNT}），暂停通过，待处理=${queuedGids.length}`);
                lastFriendApplyFullLogAt = Date.now();
            }
            return;
        }

        lastFriendApplyFullLogAt = 0;
        const gids = queuedGids.slice(0, remainingSlots);
        if (gids.length <= 0) return;
        if (gids.length < queuedGids.length) {
            log('FRIEND_APPLY', `好友位剩余 ${remainingSlots}/${FRIEND_MAX_COUNT}，本轮仅通过 ${gids.length}/${queuedGids.length} 条申请`);
        }

        const result = await acceptFriendsWithRetry(gids);
        if (!result.ok) return;

        const accepted = result.acceptedGids.length > 0 ? result.acceptedGids : gids;
        for (const gid of accepted) {
            pendingFriendApplications.delete(gid);
        }
    } finally {
        isProcessingPendingFriendApplications = false;
    }
}

async function checkAndAcceptApplications() {
    try {
        const reply = await getApplications();
        const applications = reply.applications || [];
        if (applications.length === 0) return;

        const names = applications.map(a => a.name || `GID:${toNum(a.gid)}`).join(', ');
        log('FRIEND_APPLY', `拉取到 ${applications.length} 条好友申请：${names}`);

        enqueueFriendApplications(applications);
        await processPendingFriendApplications();
    } catch (e) {
        // ignore and retry next cycle
    }
}
async function acceptFriendsWithRetry(gids) {
    if (gids.length === 0) return { ok: true, acceptedGids: [] };
    try {
        const reply = await acceptFriends(gids);
        const friends = reply.friends || [];
        let acceptedGids = [];
        if (friends.length > 0) {
            const names = friends.map(f => f.name || f.remark || `GID:${toNum(f.gid)}`).join(', ');
            log('FRIEND_APPLY', `已通过 ${friends.length} 条好友申请：${names}`);
            acceptedGids = friends.map((f) => toNum(f.gid)).filter((gid) => gid > 0);
        }
        if (acceptedGids.length <= 0) {
            acceptedGids = gids.filter((gid) => toNum(gid) > 0).map((gid) => toNum(gid));
        }
        return { ok: true, acceptedGids };
    } catch (e) {
        logWarn('FRIEND_APPLY', `通过好友申请失败：${normalizeRuntimeText(e.message)}`);
        return { ok: false, acceptedGids: [] };
    }
}

module.exports = {
    checkFriends, startFriendCheckLoop, stopFriendCheckLoop,
    checkAndAcceptApplications,
};
