const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const net = require('net');
const { spawn, spawnSync } = require('child_process');
const { pushNotification } = require('./notification-service');

const app = express();
const PORT = Number(process.env.PORT || 3737);

const WEB_ROOT = __dirname;
const BOT_ROOT = path.resolve(WEB_ROOT, '../qq-farm-bot');
const DEBUG_LAB_ROOT = path.resolve(WEB_ROOT, '../proxy-test-a-lab');
const DEBUG_LAB_ENTRY = path.join(DEBUG_LAB_ROOT, 'server.js');
const DEBUG_LAB_HOST = process.env.DEBUG_LAB_HOST || '127.0.0.1';
const DEBUG_LAB_PORT = Number(process.env.DEBUG_LAB_PORT || 3906);
const DEBUG_PROXY_HOST_FIXED = process.env.DEBUG_PROXY_HOST_FIXED || '45.207.220.16';
const DEBUG_UPSTREAM_PROXY_HOST = process.env.DEBUG_UPSTREAM_PROXY_HOST || '127.0.0.1';
const DEBUG_UPSTREAM_PROXY_PORT = Number(process.env.DEBUG_UPSTREAM_PROXY_PORT || 8080);
const DEBUG_UPSTREAM_HOSTS = Array.from(new Set([DEBUG_UPSTREAM_PROXY_HOST, DEBUG_PROXY_HOST_FIXED].filter(Boolean)));
const DEBUG_AUTO_START_MITM = !['0', 'false', 'no', 'off'].includes(String(process.env.DEBUG_AUTO_START_MITM || '1').trim().toLowerCase());
const DEBUG_MITM_CMD = String(process.env.DEBUG_MITM_CMD || 'mitmweb').trim() || 'mitmweb';
const DEBUG_MITM_STARTUP_TIMEOUT_MS = Number(process.env.DEBUG_MITM_STARTUP_TIMEOUT_MS || 15000);
const DEBUG_MITM_PROBE_TIMEOUT_MS = Number(process.env.DEBUG_MITM_PROBE_TIMEOUT_MS || 1200);
const DATA_ROOT = path.join(WEB_ROOT, 'data');
const PUBLIC_ROOT = path.join(WEB_ROOT, 'public');

const USERS_PATH = path.join(DATA_ROOT, 'users.json');
const ADMIN_SETTINGS_PATH = path.join(DATA_ROOT, 'admin-settings.json');
const SEED_SHOP_DATA_PATH = path.join(BOT_ROOT, 'tools', 'seed-shop-merged-export.json');
const FARM_CALC_PATH = path.join(BOT_ROOT, 'tools', 'calc-exp-yield.js');
const CROPS_PUBLIC_DIR = path.join(PUBLIC_ROOT, 'assets', 'crops');
const {
    getPlantById,
    getItemInfoById,
    getItemName,
} = require(path.join(BOT_ROOT, 'src', 'gameConfig'));

const DEFAULT_CONFIG = {
    platform: 'wx',
    code: '',
    clientVersion: '',
    clientVersionHistory: [],
    codeStatus: 'empty',
    codeStatusReason: '',
    codeStatusMessage: '',
    codeUpdatedAt: '',
    codeLastUsedAt: '',
    codeLastErrorAt: '',
    performanceMode: 'standard',
    intervalSec: 1,
    friendIntervalSec: 1,
    fastHarvest: true,
    autoFertilize: true,
    friendActiveStart: '',
    friendActiveEnd: '',
    friendActiveAllDay: false,
    friendApplyActiveStart: '',
    friendApplyActiveEnd: '',
    friendApplyAllDay: true,
    friendActionSteal: true,
    friendActionCare: true,
    friendActionPrank: false,
    friendStealEnabled: {},
    stealLevelThreshold: 0,
    friendAutoDeleteNoStealEnabled: false,
    friendAutoDeleteNoStealDays: 7,
    landRefreshIntervalSec: 5,
    preferredSeedId: 0,
    allowMulti: false,
    extraArgs: '',
    farmCalcUseAutoLands: true,
    farmCalcManualLands: 24,
    landUpgradeSweepRequestId: '',
    landUpgradeSweepRequestedAtMs: 0,
    mallDailyClaimRequestId: '',
    mallDailyClaimRequestedAtMs: 0,
    mallBuy10hFertRequestId: '',
    mallBuy10hFertRequestedAtMs: 0,
    mallBuy10hFertCount: 1,
    bagUseAllRequestId: '',
    bagUseAllRequestedAtMs: 0,
    bagSnapshotRequestId: '',
    bagSnapshotRequestedAtMs: 0,
    bagUseSelectedRequestId: '',
    bagUseSelectedRequestedAtMs: 0,
    bagUseSelectedItems: [],
    friendDeleteRequests: [],
    notificationChannels: {
        emailEnabled: false,
        mailTo: '',
        smtpHost: '',
        smtpPort: 465,
        smtpUser: '',
        smtpPass: '',
        smtpFromName: 'QQ Farm Bot',
        serverChanEnabled: false,
        serverChanType: 'sc3',
        serverChanKey: '',
    },
    disconnectNotify: {
        emailEnabled: false,
        serverChanEnabled: false,
    },
    reportNotify: {
        hourlyEnabled: false,
        dailyEnabled: false,
        emailEnabled: false,
        serverChanEnabled: false,
        dailyHour: 8,
    },
};

const MAX_LOG_LINES = 2000;
const MAX_READ_CHUNK = 512 * 1024;
const PROFILE_LOG_REPEAT_WINDOW_MS = 10 * 60 * 1000;
const SESSION_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const AUTO_CODE_FILE_PATH = process.env.AUTO_CODE_FILE_PATH || '/opt/mitm/code.txt';
const AUTO_CODE_LISTEN_MS = 3 * 60 * 1000;

let farmCalcModule = null;
let farmCalcModuleError = null;
let debugLabProcess = null;
let debugMitmProcess = null;
let debugMitmStartingPromise = null;
let autoCodeSession = null; // { owner, startedAtMs, endsAtMs, deleteWarning }
let autoCodeTickTimer = null;
const autoCodeResultByUser = new Map(); // username => { code, clientVersion, reason, atIso, note }
let seedUnitValueCache = { mtimeMs: -1, map: new Map() };
let cropIconMapCache = null;
const overviewStatsCache = new Map();
const disconnectNotifyState = new Map();
const hourlyReportTimers = new Map();
const dailyReportTimers = new Map();
const WAREHOUSE_SUPER_FRUIT_ID_OFFSET = 1000000;

function getWarehouseBaseItemId(itemId, explicitBaseId = 0) {
    const id = Math.floor(Number(itemId) || 0);
    const baseId = Math.floor(Number(explicitBaseId) || 0);
    if (baseId > 0) return baseId;
    if (id >= WAREHOUSE_SUPER_FRUIT_ID_OFFSET) {
        const derived = id % WAREHOUSE_SUPER_FRUIT_ID_OFFSET;
        if (derived > 0) return derived;
    }
    return id;
}

function normalizeBagSnapshotItem(raw) {
    if (!raw || typeof raw !== 'object') return null;

    const id = Math.floor(Number(raw.id) || 0);
    const count = Math.floor(Number(raw.count) || 0);
    if (id <= 0 || count <= 0) return null;

    const baseId = getWarehouseBaseItemId(id, raw.baseId);
    const itemInfo = getItemInfoById(id) || (baseId > 0 && baseId !== id ? getItemInfoById(baseId) : null);
    const exactName = String((raw && raw.name) || '').trim();
    const fallbackName = (baseId > 0 ? getItemName(baseId) : '') || getItemName(id);
    const normalizedName = (!exactName || exactName === '未知物品' || exactName === '鏈煡鐗╁搧')
        ? fallbackName
        : exactName;

    return {
        ...raw,
        id,
        baseId,
        count,
        uid: Math.max(0, Math.floor(Number(raw.uid) || 0)),
        name: normalizedName || exactName || `道具${id}`,
        itemType: raw.itemType == null ? Math.floor(Number(itemInfo && itemInfo.type) || 0) : raw.itemType,
        interactionType: raw.interactionType == null ? String((itemInfo && itemInfo.interaction_type) || '').trim() : raw.interactionType,
        canUse: raw.canUse == null ? Math.floor(Number(itemInfo && itemInfo.can_use) || 0) : raw.canUse,
        isSuperFruit: raw.isSuperFruit == null ? (baseId > 0 && baseId !== id) : raw.isSuperFruit,
    };
}

function normalizeBagSnapshotList(list) {
    return (Array.isArray(list) ? list : [])
        .map(normalizeBagSnapshotItem)
        .filter(Boolean);
}

function getCropIconMap() {
    if (cropIconMapCache) return cropIconMapCache;
    const map = new Map();
    try {
        if (fs.existsSync(CROPS_PUBLIC_DIR)) {
            const files = fs.readdirSync(CROPS_PUBLIC_DIR);
            for (const file of files) {
                if (!/\.(png|jpe?g|webp|svg)$/i.test(file)) continue;
                const leadingMatch = file.match(/^(\d+)_/);
                if (leadingMatch) map.set(Number(leadingMatch[1]), file);
                const cropMatch = file.match(/Crop_(\d+)/i);
                if (cropMatch) map.set(Number(cropMatch[1]), file);
            }
        }
    } catch (_err) {
        // ignore crop icon scan failure
    }
    cropIconMapCache = map;
    return cropIconMapCache;
}

function getCropIconFile(plantId) {
    const pid = Number(plantId) || 0;
    if (pid <= 0) return '';

    const iconMap = getCropIconMap();
    if (iconMap.has(pid)) return iconMap.get(pid) || '';

    const plantInfo = getPlantById(pid);
    if (plantInfo && plantInfo.seed_id) {
        const seedId = Number(plantInfo.seed_id) || 0;
        if (seedId > 0 && iconMap.has(seedId)) return iconMap.get(seedId) || '';
    }

    const shortId = pid % 100000;
    if (shortId > 0 && iconMap.has(shortId)) return iconMap.get(shortId) || '';
    return '';
}

function decorateLandsSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== 'object' || !Array.isArray(snapshot.lands)) return snapshot;
    const lands = snapshot.lands.map((land) => {
        if (!land || typeof land !== 'object') return land;
        const nextLand = { ...land };
        const plant = land.plant && typeof land.plant === 'object'
            ? { ...land.plant }
            : null;
        const iconFile = plant ? getCropIconFile(plant.id) : '';
        if (plant) {
            plant.iconFile = iconFile;
            nextLand.plant = plant;
        }
        nextLand.iconFile = iconFile;
        return nextLand;
    });
    return { ...snapshot, lands };
}

// ============ Session / Auth ============

const sessions = new Map(); // token -> { username, createdAt }
const userStates = new Map(); // username -> userState

function getUserDataDir(username) {
    return path.join(DATA_ROOT, 'users', username);
}

function createUserState(username) {
    const dataDir = getUserDataDir(username);
    return {
        username,
        runtime: {
            running: false,
            pid: null,
            startedAt: null,
            lastAction: 'idle',
            lastOutput: '',
            lastError: '',
        },
        metrics: createEmptyMetrics(),
        logs: [],
        nextLogId: 1,
        logOffset: 0,
        logRemainder: '',
        clients: new Set(),
        runtimeSig: '',
        profileLogEcho: { signature: '', at: 0 },
        dataDir,
        configPath: path.join(dataDir, 'config.json'),
        logPath: path.join(dataDir, 'logs', 'farm.log'),
        landsPath: path.join(dataDir, 'lands.json'),
        bagSnapshotPath: path.join(dataDir, 'bag-items.json'),
        sharePath: path.join(dataDir, 'share.txt'),
        lockPath: path.join(dataDir, '.bot.lock'),
    };
}

function getUserState(username) {
    if (!userStates.has(username)) {
        userStates.set(username, createUserState(username));
    }
    return userStates.get(username);
}

function loadUsers() {
    if (!fs.existsSync(USERS_PATH)) return [];
    try {
        const data = JSON.parse(fs.readFileSync(USERS_PATH, 'utf8'));
        return Array.isArray(data) ? data : [];
    } catch (e) { return []; }
}

function saveUsers(users) {
    fs.mkdirSync(DATA_ROOT, { recursive: true });
    fs.writeFileSync(USERS_PATH, JSON.stringify(users, null, 2) + '\n', 'utf8');
}

function readAdminSettings() {
    try {
        if (fs.existsSync(ADMIN_SETTINGS_PATH)) {
            const data = JSON.parse(fs.readFileSync(ADMIN_SETTINGS_PATH, 'utf8'));
            if (data && typeof data === 'object') return data;
        }
    } catch (e) { /* ignore */ }
    return { registrationEnabled: true };
}

function saveAdminSettings(settings) {
    fs.mkdirSync(DATA_ROOT, { recursive: true });
    fs.writeFileSync(ADMIN_SETTINGS_PATH, JSON.stringify(settings, null, 2) + '\n', 'utf8');
}

function hashPassword(password) {
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = crypto.scryptSync(password, salt, 64).toString('hex');
    return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
    try {
        const [salt, hash] = stored.split(':');
        const check = crypto.scryptSync(password, salt, 64).toString('hex');
        return check === hash;
    } catch (e) { return false; }
}

function generateToken() {
    return crypto.randomBytes(32).toString('hex');
}

function parseCookies(req) {
    const cookies = {};
    const header = req.headers.cookie || '';
    header.split(';').forEach((part) => {
        const idx = part.indexOf('=');
        if (idx < 0) return;
        const k = part.slice(0, idx).trim();
        const v = part.slice(idx + 1).trim();
        if (k) cookies[k] = decodeURIComponent(v);
    });
    return cookies;
}

function requireAuth(req, res, next) {
    const token = parseCookies(req).session;
    if (!token) return res.status(401).json({ error: '未登录' });
    const session = sessions.get(token);
    if (!session) return res.status(401).json({ error: '会话已过期，请重新登录' });
    req.username = session.username;
    req.userState = getUserState(session.username);
    next();
}

function requireAdmin(req, res, next) {
    const token = parseCookies(req).session;
    if (!token) return res.status(401).json({ error: '未登录' });
    const session = sessions.get(token);
    if (!session) return res.status(401).json({ error: '会话已过期，请重新登录' });
    const users = loadUsers();
    const user = users.find((u) => u.username === session.username);
    if (!user || !user.isAdmin) return res.status(403).json({ error: '无权限，仅管理员可访问' });
    req.username = session.username;
    req.userState = getUserState(session.username);
    next();
}

function setCookieHeader(res, token) {
    res.setHeader(
        'Set-Cookie',
        `session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${Math.floor(SESSION_MAX_AGE_MS / 1000)}`
    );
}

function clearCookieHeader(res) {
    res.setHeader('Set-Cookie', 'session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0');
}

// ============ Helpers ============

function createEmptyMetrics() {
    return {
        harvest: 0,
        steal: 0,
        water: 0,
        weed: 0,
        bug: 0,
        taskClaims: 0,
        soldItems: 0,
        soldBatches: 0,
        errorCount: 0,
        level: null,
        gold: null,
        diamond: null,
        landCount: null,
        landCountUpdatedAt: null,
        expCurrent: null,
        expNeeded: null,
        normalFertilizerCount: null,
        normalFertilizerState: null,
        normalFertilizerUpdatedAt: null,
        lastTask: '',
        lastError: '',
        lastUpdateAt: null,
    };
}

function toInt(value, fallback, min, max) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    const i = Math.floor(n);
    return Math.min(Math.max(i, min), max);
}

function toBoolean(value, fallback) {
    if (value === undefined || value === null || value === '') return fallback;
    const text = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(text)) return true;
    if (['0', 'false', 'no', 'off'].includes(text)) return false;
    return fallback;
}

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractAutoCodePayload(raw) {
    const lines = String(raw || '')
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    return {
        code: String(lines[0] || '').slice(0, 1024),
        clientVersion: String(lines[1] || '').slice(0, 120),
    };
}

function readAutoCodeFromFile() {
    try {
        if (!fs.existsSync(AUTO_CODE_FILE_PATH)) {
            return { code: '', clientVersion: '' };
        }
        const raw = fs.readFileSync(AUTO_CODE_FILE_PATH, 'utf8');
        return extractAutoCodePayload(raw);
    } catch (err) {
        return { code: '', clientVersion: '' };
    }
}

function clearAutoCodeFile() {
    try {
        if (fs.existsSync(AUTO_CODE_FILE_PATH)) {
            fs.unlinkSync(AUTO_CODE_FILE_PATH);
        }
        return { ok: true, error: '' };
    } catch (err) {
        return { ok: false, error: err.message || String(err) };
    }
}

function stopAutoCodeTicker() {
    if (autoCodeTickTimer) {
        clearInterval(autoCodeTickTimer);
        autoCodeTickTimer = null;
    }
}

function finishAutoCodeSession(reason, payload = {}) {
    if (!autoCodeSession) return;
    const owner = autoCodeSession.owner;
    const note = autoCodeSession.deleteWarning || '';
    const result = {
        code: String((payload && payload.code) || ''),
        clientVersion: String((payload && payload.clientVersion) || ''),
        reason: String(reason || ''),
        note,
        atIso: new Date().toISOString(),
    };
    autoCodeResultByUser.set(owner, result);
    autoCodeSession = null;
    stopAutoCodeTicker();
}

function tickAutoCodeSession() {
    if (!autoCodeSession) {
        stopAutoCodeTicker();
        return;
    }

    const now = Date.now();
    if (now >= autoCodeSession.endsAtMs) {
        finishAutoCodeSession('timeout', {});
        return;
    }

    const payload = readAutoCodeFromFile();
    if (payload.code) {
        finishAutoCodeSession('captured', payload);
    }
}

function startAutoCodeSession(username, deleteWarning = '') {
    autoCodeSession = {
        owner: username,
        startedAtMs: Date.now(),
        endsAtMs: Date.now() + AUTO_CODE_LISTEN_MS,
        deleteWarning: String(deleteWarning || ''),
    };
    stopAutoCodeTicker();
    autoCodeTickTimer = setInterval(() => {
        tickAutoCodeSession();
    }, 1000);
    tickAutoCodeSession();
    return autoCodeSession;
}

function consumeAutoCodeResult(username) {
    const key = String(username || '');
    if (!key) return null;
    const value = autoCodeResultByUser.get(key) || null;
    if (value) {
        autoCodeResultByUser.delete(key);
    }
    return value;
}

function getAutoCodeStatusForUser(username, { consumeResult = true } = {}) {
    const user = String(username || '');
    const session = autoCodeSession;
    const active = Boolean(session);
    const mine = Boolean(active && session.owner === user);
    const lockedByOther = Boolean(active && session.owner !== user);
    const remainingMs = active ? Math.max(0, Number(session.endsAtMs || 0) - Date.now()) : 0;
    const result = consumeResult
        ? consumeAutoCodeResult(user)
        : (autoCodeResultByUser.get(user) || null);
    const sessionNote = mine ? String((session && session.deleteWarning) || '') : '';

    return {
        active,
        mine,
        lockedByOther,
        remainingMs,
        startedAtMs: active ? Number(session.startedAtMs || 0) : 0,
        endsAtMs: active ? Number(session.endsAtMs || 0) : 0,
        code: result && result.code ? String(result.code) : '',
        clientVersion: result && result.clientVersion ? String(result.clientVersion) : '',
        resultReason: result && result.reason ? String(result.reason) : '',
        note: result && result.note ? String(result.note) : sessionNote,
        filePath: AUTO_CODE_FILE_PATH,
    };
}

function isDebugLabProcessAlive() {
    return Boolean(debugLabProcess && debugLabProcess.exitCode === null && !debugLabProcess.killed);
}

function isDebugMitmProcessAlive() {
    return Boolean(debugMitmProcess && debugMitmProcess.exitCode === null && !debugMitmProcess.killed);
}

function splitShellArgs(text = '') {
    const result = [];
    const source = String(text || '');
    const re = /"([^"]*)"|'([^']*)'|(\S+)/g;
    let match;
    while ((match = re.exec(source)) !== null) {
        result.push(match[1] ?? match[2] ?? match[3]);
    }
    return result;
}

function getDebugMitmArgs() {
    const fromEnv = String(process.env.DEBUG_MITM_ARGS || '').trim();
    if (fromEnv) return splitShellArgs(fromEnv);

    const webHost = String(process.env.DEBUG_MITM_WEB_HOST || '0.0.0.0').trim() || '0.0.0.0';
    const webPort = Number(process.env.DEBUG_MITM_WEB_PORT || 8081);
    const listenHost = String(process.env.DEBUG_MITM_LISTEN_HOST || '0.0.0.0').trim() || '0.0.0.0';
    const blockGlobal = String(process.env.DEBUG_MITM_BLOCK_GLOBAL || 'false').trim() || 'false';
    const confDir = String(process.env.DEBUG_MITM_CONFDIR || '/opt/mitm').trim() || '/opt/mitm';

    const args = [
        '--web-host', webHost,
        '--web-port', String(webPort),
        '--listen-host', listenHost,
        '--listen-port', String(DEBUG_UPSTREAM_PROXY_PORT),
        '--set', `block_global=${blockGlobal}`,
        '--set', `confdir=${confDir}`,
    ];

    const extra = splitShellArgs(String(process.env.DEBUG_MITM_EXTRA_ARGS || '').trim());
    if (extra.length > 0) args.push(...extra);
    return args;
}

function probeTcp(host, port, timeoutMs = DEBUG_MITM_PROBE_TIMEOUT_MS) {
    return new Promise((resolve) => {
        const socket = new net.Socket();
        let settled = false;

        const done = (ok) => {
            if (settled) return;
            settled = true;
            try { socket.destroy(); } catch (e) { }
            resolve(ok);
        };

        socket.setTimeout(Math.max(200, Number(timeoutMs) || 1200));
        socket.once('connect', () => done(true));
        socket.once('timeout', () => done(false));
        socket.once('error', () => done(false));

        try {
            socket.connect(Number(port), String(host));
        } catch (e) {
            done(false);
        }
    });
}

async function isAnyDebugUpstreamReachable() {
    for (const host of DEBUG_UPSTREAM_HOSTS) {
        if (!host) continue;
        // eslint-disable-next-line no-await-in-loop
        const ok = await probeTcp(host, DEBUG_UPSTREAM_PROXY_PORT, DEBUG_MITM_PROBE_TIMEOUT_MS);
        if (ok) return true;
    }
    return false;
}

async function spawnDebugMitmProcess() {
    if (isDebugMitmProcessAlive()) return { startedNow: false };
    if (debugMitmStartingPromise) return debugMitmStartingPromise;

    debugMitmStartingPromise = new Promise((resolve, reject) => {
        let child = null;
        let settled = false;
        const args = getDebugMitmArgs();

        const finish = (err) => {
            if (settled) return;
            settled = true;
            debugMitmStartingPromise = null;
            if (err) {
                reject(err);
                return;
            }
            resolve({ startedNow: true });
        };

        try {
            child = spawn(DEBUG_MITM_CMD, args, {
                cwd: WEB_ROOT,
                detached: false,
                stdio: 'ignore',
                env: { ...process.env },
            });
        } catch (err) {
            const wrapped = new Error(`failed to spawn mitm: ${err.message || String(err)}`);
            wrapped.statusCode = 500;
            finish(wrapped);
            return;
        }

        child.once('error', (err) => {
            if (debugMitmProcess === child) debugMitmProcess = null;
            const wrapped = new Error(`failed to start mitm (${DEBUG_MITM_CMD}): ${err.message || String(err)}`);
            wrapped.statusCode = 500;
            finish(wrapped);
        });

        child.once('spawn', () => {
            debugMitmProcess = child;
            child.on('exit', () => {
                if (debugMitmProcess === child) {
                    debugMitmProcess = null;
                }
            });
            finish(null);
        });
    });

    return debugMitmStartingPromise;
}

async function ensureDebugMitmReady(userState) {
    if (await isAnyDebugUpstreamReachable()) {
        return { autoStarted: false };
    }

    if (!DEBUG_AUTO_START_MITM) {
        const err = new Error(`upstream proxy is unavailable on ${DEBUG_UPSTREAM_HOSTS.join(',')}:${DEBUG_UPSTREAM_PROXY_PORT}`);
        err.statusCode = 503;
        throw err;
    }

    const spawnResult = await spawnDebugMitmProcess();
    if (spawnResult && spawnResult.startedNow) {
        addUserLogLine(
            userState,
            `[WEBUI] 已自动启动 mitm: ${DEBUG_MITM_CMD} (listen ${DEBUG_UPSTREAM_PROXY_PORT})`,
            true
        );
    }

    const deadline = Date.now() + Math.max(3000, DEBUG_MITM_STARTUP_TIMEOUT_MS);
    while (Date.now() < deadline) {
        // eslint-disable-next-line no-await-in-loop
        if (await isAnyDebugUpstreamReachable()) {
            return { autoStarted: Boolean(spawnResult && spawnResult.startedNow) };
        }
        // eslint-disable-next-line no-await-in-loop
        await delay(300);
    }

    const err = new Error(`mitm startup timeout: ${DEBUG_UPSTREAM_HOSTS.join(',')}:${DEBUG_UPSTREAM_PROXY_PORT} not reachable`);
    err.statusCode = 504;
    throw err;
}

function buildDebugLabApiPath(apiPath, query = null) {
    if (!query || typeof query !== 'object') return apiPath;
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
        if (value === undefined || value === null || value === '') continue;
        params.set(key, String(value));
    }
    const q = params.toString();
    if (!q) return apiPath;
    return apiPath.includes('?') ? `${apiPath}&${q}` : `${apiPath}?${q}`;
}

function requestDebugLab(method, apiPath, body = null, timeoutMs = 6000, query = null) {
    return new Promise((resolve, reject) => {
        const payload = body ? JSON.stringify(body) : '';
        const req = http.request({
            host: DEBUG_LAB_HOST,
            port: DEBUG_LAB_PORT,
            path: buildDebugLabApiPath(apiPath, query),
            method,
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload),
            },
        }, (res) => {
            const chunks = [];
            res.on('data', (chunk) => chunks.push(chunk));
            res.on('end', () => {
                const text = Buffer.concat(chunks).toString('utf8');
                let data = {};
                if (text) {
                    try {
                        data = JSON.parse(text);
                    } catch (e) {
                        const err = new Error(`debug-lab response is not json: ${text.slice(0, 120)}`);
                        err.statusCode = 502;
                        reject(err);
                        return;
                    }
                }
                if (res.statusCode >= 400) {
                    const err = new Error(data.error || data.message || `debug-lab request failed: ${res.statusCode}`);
                    err.statusCode = res.statusCode;
                    reject(err);
                    return;
                }
                resolve(data);
            });
        });

        req.setTimeout(timeoutMs, () => {
            req.destroy(new Error('debug-lab request timeout'));
        });
        req.on('error', (err) => reject(err));
        if (payload) req.write(payload);
        req.end();
    });
}

async function queryDebugLabStatus(userId = '') {
    const data = await requestDebugLab(
        'GET',
        '/api/status',
        null,
        3000,
        userId ? { user: userId } : null
    );
    return {
        available: true,
        status: data.status || null,
        requests: Array.isArray(data.requests) ? data.requests : [],
    };
}

function spawnDebugLabProcess() {
    if (!fs.existsSync(DEBUG_LAB_ENTRY)) {
        const err = new Error(`missing debug lab entry: ${DEBUG_LAB_ENTRY}`);
        err.statusCode = 500;
        throw err;
    }

    if (isDebugLabProcessAlive()) return;

    const child = spawn(process.execPath, [DEBUG_LAB_ENTRY], {
        cwd: DEBUG_LAB_ROOT,
        detached: false,
        stdio: 'ignore',
        env: { ...process.env, PORT: String(DEBUG_LAB_PORT) },
    });
    child.on('exit', () => {
        if (debugLabProcess === child) {
            debugLabProcess = null;
        }
    });
    debugLabProcess = child;
}

async function ensureDebugLabRunning() {
    try {
        return await queryDebugLabStatus();
    } catch (firstErr) {
        // not ready yet
    }

    spawnDebugLabProcess();
    const deadline = Date.now() + 10000;
    let lastErr = null;
    while (Date.now() < deadline) {
        try {
            return await queryDebugLabStatus();
        } catch (err) {
            lastErr = err;
            await delay(300);
        }
    }
    const err = new Error(`debug-lab start timeout${lastErr ? `: ${lastErr.message}` : ''}`);
    err.statusCode = 504;
    throw err;
}

function terminateDebugLabProcess() {
    if (!isDebugLabProcessAlive()) return;
    try { debugLabProcess.kill('SIGTERM'); } catch (e) { }
    setTimeout(() => {
        if (isDebugLabProcessAlive()) {
            try { debugLabProcess.kill('SIGKILL'); } catch (e) { }
        }
    }, 800);
}

function terminateDebugMitmProcess() {
    if (debugMitmStartingPromise) {
        // no-op: wait for child reference to become available
    }
    if (!isDebugMitmProcessAlive()) return;
    try { debugMitmProcess.kill('SIGTERM'); } catch (e) { }
    setTimeout(() => {
        if (isDebugMitmProcessAlive()) {
            try { debugMitmProcess.kill('SIGKILL'); } catch (e) { }
        }
    }, 800);
}

function normalizeTimeHHMM(value) {
    const s = String(value || '').trim();
    return /^\d{2}:\d{2}$/.test(s) ? s : '';
}

function normalizePerformanceMode(value) {
    const key = String(value || '').trim().toLowerCase();
    if (['standard', 'retire', 'berserk'].includes(key)) return key;
    return DEFAULT_CONFIG.performanceMode;
}

function normalizeFriendStealEnabledMap(raw) {
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

function normalizeCodeStatus(value, fallback = 'empty') {
    const status = String(value || '').trim().toLowerCase();
    if (['empty', 'ready', 'active', 'error'].includes(status)) return status;
    return fallback;
}

function normalizeClientVersion(value) {
    return String(value || '').trim().slice(0, 120);
}

function normalizeClientVersionHistory(raw, currentValue = '') {
    const list = [];
    const seen = new Set();
    const pushOne = (value) => {
        const normalized = normalizeClientVersion(value);
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        list.push(normalized);
    };
    pushOne(currentValue);
    if (Array.isArray(raw)) {
        for (const one of raw) {
            if (list.length >= 20) break;
            pushOne(one);
        }
    }
    return list.slice(0, 20);
}

function normalizeBagUseSelectedItems(raw) {
    if (!Array.isArray(raw)) return [];
    const items = [];
    for (const one of raw) {
        if (!one || typeof one !== 'object') continue;
        if (items.length >= 500) break;
        const id = toInt(one.id, 0, 1, 999999999);
        if (id <= 0) continue;
        const uid = toInt(one.uid, 0, 0, 9999999999999);
        const count = toInt(one.count, 1, 1, 999999999);
        const name = String(one.name || '').trim().slice(0, 80);
        items.push({
            id,
            uid,
            count,
            name,
        });
    }
    return items;
}

function normalizeFriendDeleteRequests(raw) {
    if (!Array.isArray(raw)) return [];
    const rows = [];
    const seen = new Set();
    for (const one of raw) {
        if (!one || typeof one !== 'object') continue;
        if (rows.length >= 50) break;
        const requestId = String(one.requestId || '').trim().slice(0, 80);
        const gid = toInt(one.gid, 0, 1, 9999999999999);
        if (!requestId || gid <= 0 || seen.has(requestId)) continue;
        seen.add(requestId);
        rows.push({
            requestId,
            gid,
            name: String(one.name || '').trim().slice(0, 80),
            requestedAtMs: toInt(one.requestedAtMs, 0, 0, 9999999999999),
        });
    }
    return rows;
}

function normalizeConfig(raw = {}) {
    const platform = 'wx';
    const code = String(raw.code || '').trim();
    const clientVersionSource = Object.prototype.hasOwnProperty.call(raw, 'clientVersion')
        ? raw.clientVersion
        : DEFAULT_CONFIG.clientVersion;
    const clientVersion = normalizeClientVersion(clientVersionSource);
    const clientVersionHistory = normalizeClientVersionHistory(raw.clientVersionHistory, clientVersion);
    const codeStatusFallback = (code && clientVersion) ? 'ready' : 'empty';
    const codeStatus = normalizeCodeStatus(raw.codeStatus, codeStatusFallback);
    const codeStatusReason = String(raw.codeStatusReason || '').trim().slice(0, 64);
    const codeStatusMessage = String(raw.codeStatusMessage || '').trim().slice(0, 300);
    const codeUpdatedAt = String(raw.codeUpdatedAt || '').trim().slice(0, 64);
    const codeLastUsedAt = String(raw.codeLastUsedAt || '').trim().slice(0, 64);
    const codeLastErrorAt = String(raw.codeLastErrorAt || '').trim().slice(0, 64);
    const performanceMode = normalizePerformanceMode(raw.performanceMode);
    const intervalSec = toInt(raw.intervalSec, DEFAULT_CONFIG.intervalSec, 0, 300);
    const friendIntervalSec = toInt(raw.friendIntervalSec, DEFAULT_CONFIG.friendIntervalSec, 0, 300);
    const fastHarvest = toBoolean(raw.fastHarvest, DEFAULT_CONFIG.fastHarvest);
    const autoFertilize = toBoolean(raw.autoFertilize, DEFAULT_CONFIG.autoFertilize);
    const friendActiveStart = normalizeTimeHHMM(raw.friendActiveStart);
    const friendActiveEnd = normalizeTimeHHMM(raw.friendActiveEnd);
    const friendActiveAllDay = toBoolean(raw.friendActiveAllDay, DEFAULT_CONFIG.friendActiveAllDay);
    const friendApplyActiveStart = normalizeTimeHHMM(raw.friendApplyActiveStart);
    const friendApplyActiveEnd = normalizeTimeHHMM(raw.friendApplyActiveEnd);
    const friendApplyAllDay = toBoolean(raw.friendApplyAllDay, DEFAULT_CONFIG.friendApplyAllDay);
    const friendActionSteal = toBoolean(raw.friendActionSteal, DEFAULT_CONFIG.friendActionSteal);
    const legacyWater = toBoolean(raw.friendActionWater, true);
    const legacyWeed = toBoolean(raw.friendActionWeed, true);
    const legacyBug = toBoolean(raw.friendActionBug, true);
    const hasLegacyCareFields =
        Object.prototype.hasOwnProperty.call(raw, 'friendActionWater')
        || Object.prototype.hasOwnProperty.call(raw, 'friendActionWeed')
        || Object.prototype.hasOwnProperty.call(raw, 'friendActionBug');
    const friendActionCare = toBoolean(
        raw.friendActionCare,
        hasLegacyCareFields ? (legacyWater || legacyWeed || legacyBug) : DEFAULT_CONFIG.friendActionCare
    );
    const friendActionPrank = toBoolean(raw.friendActionPrank, DEFAULT_CONFIG.friendActionPrank);
    const friendStealEnabled = normalizeFriendStealEnabledMap(raw.friendStealEnabled);
    const stealLevelThreshold = toInt(raw.stealLevelThreshold, DEFAULT_CONFIG.stealLevelThreshold, 0, 999);
    const friendAutoDeleteNoStealEnabled = toBoolean(raw.friendAutoDeleteNoStealEnabled, DEFAULT_CONFIG.friendAutoDeleteNoStealEnabled);
    const friendAutoDeleteNoStealDays = toInt(raw.friendAutoDeleteNoStealDays, DEFAULT_CONFIG.friendAutoDeleteNoStealDays, 1, 3650);
    const landRefreshIntervalSec = toInt(raw.landRefreshIntervalSec, DEFAULT_CONFIG.landRefreshIntervalSec, 1, 300);
    const preferredSeedId = toInt(raw.preferredSeedId, DEFAULT_CONFIG.preferredSeedId, 0, 99999999);
    const allowMulti = Boolean(raw.allowMulti);
    const extraArgs = String(raw.extraArgs || '').trim().slice(0, 500);
    const farmCalcUseAutoLands = toBoolean(raw.farmCalcUseAutoLands, DEFAULT_CONFIG.farmCalcUseAutoLands);
    const farmCalcManualLands = toInt(raw.farmCalcManualLands, DEFAULT_CONFIG.farmCalcManualLands, 1, 500);
    const landUpgradeSweepRequestId = String(raw.landUpgradeSweepRequestId || '').trim().slice(0, 80);
    const landUpgradeSweepRequestedAtMs = toInt(raw.landUpgradeSweepRequestedAtMs, DEFAULT_CONFIG.landUpgradeSweepRequestedAtMs, 0, 9999999999999);
    const mallDailyClaimRequestId = String(raw.mallDailyClaimRequestId || '').trim().slice(0, 80);
    const mallDailyClaimRequestedAtMs = toInt(raw.mallDailyClaimRequestedAtMs, DEFAULT_CONFIG.mallDailyClaimRequestedAtMs, 0, 9999999999999);
    const mallBuy10hFertRequestId = String(raw.mallBuy10hFertRequestId || '').trim().slice(0, 80);
    const mallBuy10hFertRequestedAtMs = toInt(raw.mallBuy10hFertRequestedAtMs, DEFAULT_CONFIG.mallBuy10hFertRequestedAtMs, 0, 9999999999999);
    const mallBuy10hFertCount = toInt(raw.mallBuy10hFertCount, DEFAULT_CONFIG.mallBuy10hFertCount, 1, 999);
    const bagUseAllRequestId = String(raw.bagUseAllRequestId || '').trim().slice(0, 80);
    const bagUseAllRequestedAtMs = toInt(raw.bagUseAllRequestedAtMs, DEFAULT_CONFIG.bagUseAllRequestedAtMs, 0, 9999999999999);
    const bagSnapshotRequestId = String(raw.bagSnapshotRequestId || '').trim().slice(0, 80);
    const bagSnapshotRequestedAtMs = toInt(raw.bagSnapshotRequestedAtMs, DEFAULT_CONFIG.bagSnapshotRequestedAtMs, 0, 9999999999999);
    const bagUseSelectedRequestId = String(raw.bagUseSelectedRequestId || '').trim().slice(0, 80);
    const bagUseSelectedRequestedAtMs = toInt(raw.bagUseSelectedRequestedAtMs, DEFAULT_CONFIG.bagUseSelectedRequestedAtMs, 0, 9999999999999);
    const bagUseSelectedItems = normalizeBagUseSelectedItems(raw.bagUseSelectedItems);
    const friendDeleteRequests = normalizeFriendDeleteRequests(raw.friendDeleteRequests);
    const notificationChannels = normalizeNotificationChannels(raw.notificationChannels || {});
    const disconnectNotify = normalizeDisconnectNotify(raw.disconnectNotify || {});
    const reportNotify = normalizeReportNotify(raw.reportNotify || {});
    return {
        platform,
        code,
        clientVersion,
        clientVersionHistory,
        codeStatus,
        codeStatusReason,
        codeStatusMessage,
        codeUpdatedAt,
        codeLastUsedAt,
        codeLastErrorAt,
        performanceMode,
        intervalSec,
        friendIntervalSec,
        fastHarvest,
        autoFertilize,
        friendActiveStart,
        friendActiveEnd,
        friendActiveAllDay,
        friendApplyActiveStart,
        friendApplyActiveEnd,
        friendApplyAllDay,
        friendActionSteal,
        friendActionCare,
        friendActionPrank,
        friendStealEnabled,
        stealLevelThreshold,
        friendAutoDeleteNoStealEnabled,
        friendAutoDeleteNoStealDays,
        landRefreshIntervalSec,
        preferredSeedId,
        allowMulti,
        extraArgs,
        farmCalcUseAutoLands,
        farmCalcManualLands,
        landUpgradeSweepRequestId,
        landUpgradeSweepRequestedAtMs,
        mallDailyClaimRequestId,
        mallDailyClaimRequestedAtMs,
        mallBuy10hFertRequestId,
        mallBuy10hFertRequestedAtMs,
        mallBuy10hFertCount,
        bagUseAllRequestId,
        bagUseAllRequestedAtMs,
        bagSnapshotRequestId,
        bagSnapshotRequestedAtMs,
        bagUseSelectedRequestId,
        bagUseSelectedRequestedAtMs,
        bagUseSelectedItems,
        friendDeleteRequests,
        notificationChannels,
        disconnectNotify,
        reportNotify,
    };
}

function buildClientConfig(config) {
    return normalizeConfig(config || {});
}

function normalizeNotificationChannels(raw = {}) {
    return {
        emailEnabled: toBoolean(raw.emailEnabled, DEFAULT_CONFIG.notificationChannels.emailEnabled),
        mailTo: String(raw.mailTo || '').trim().slice(0, 200),
        smtpHost: String(raw.smtpHost || '').trim().slice(0, 200),
        smtpPort: toInt(raw.smtpPort, DEFAULT_CONFIG.notificationChannels.smtpPort, 1, 65535),
        smtpUser: String(raw.smtpUser || '').trim().slice(0, 200),
        smtpPass: String(raw.smtpPass || '').trim().slice(0, 200),
        smtpFromName: String(raw.smtpFromName || '').trim().slice(0, 80) || DEFAULT_CONFIG.notificationChannels.smtpFromName,
        serverChanEnabled: toBoolean(raw.serverChanEnabled, DEFAULT_CONFIG.notificationChannels.serverChanEnabled),
        serverChanType: ['sc3', 'turbo'].includes(String(raw.serverChanType || '').trim().toLowerCase())
            ? String(raw.serverChanType || '').trim().toLowerCase()
            : DEFAULT_CONFIG.notificationChannels.serverChanType,
        serverChanKey: String(raw.serverChanKey || '').trim().slice(0, 200),
    };
}

function normalizeDisconnectNotify(raw = {}) {
    return {
        emailEnabled: toBoolean(raw.emailEnabled, DEFAULT_CONFIG.disconnectNotify.emailEnabled),
        serverChanEnabled: toBoolean(raw.serverChanEnabled, DEFAULT_CONFIG.disconnectNotify.serverChanEnabled),
    };
}

function normalizeReportNotify(raw = {}) {
    return {
        hourlyEnabled: toBoolean(raw.hourlyEnabled, DEFAULT_CONFIG.reportNotify.hourlyEnabled),
        dailyEnabled: toBoolean(raw.dailyEnabled, DEFAULT_CONFIG.reportNotify.dailyEnabled),
        emailEnabled: toBoolean(raw.emailEnabled, DEFAULT_CONFIG.reportNotify.emailEnabled),
        serverChanEnabled: toBoolean(raw.serverChanEnabled, DEFAULT_CONFIG.reportNotify.serverChanEnabled),
        dailyHour: toInt(raw.dailyHour, DEFAULT_CONFIG.reportNotify.dailyHour, 0, 23),
    };
}

function getDisconnectNotifyReadiness(config = {}) {
    const channels = normalizeNotificationChannels(config.notificationChannels || {});
    const toggles = normalizeDisconnectNotify(config.disconnectNotify || {});
    const reasons = [];
    let readyChannelCount = 0;

    if (!toggles.emailEnabled && !toggles.serverChanEnabled) {
        return {
            channels,
            toggles,
            readyChannelCount,
            reasons: ['掉线提醒未启用'],
        };
    }

    if (toggles.emailEnabled) {
        if (!channels.emailEnabled) {
            reasons.push('已勾选邮箱提醒，但“全局通知渠道配置 -> 邮件渠道”未启用');
        } else if (!channels.mailTo) {
            reasons.push('已勾选邮箱提醒，但未填写接收邮箱');
        } else if (!channels.smtpHost || !channels.smtpPort || !channels.smtpUser || !channels.smtpPass) {
            reasons.push('已勾选邮箱提醒，但 SMTP 配置不完整');
        } else {
            readyChannelCount += 1;
        }
    }

    if (toggles.serverChanEnabled) {
        if (!channels.serverChanEnabled) {
            reasons.push('已勾选 Server酱 提醒，但“全局通知渠道配置 -> Server酱渠道”未启用');
        } else if (!channels.serverChanKey) {
            reasons.push('已勾选 Server酱 提醒，但未填写 SendKey');
        } else {
            readyChannelCount += 1;
        }
    }

    return {
        channels,
        toggles,
        readyChannelCount,
        reasons,
    };
}

// ============ Per-User Config / Share / Lands ============

function readUserConfig(userState) {
    fs.mkdirSync(userState.dataDir, { recursive: true });
    if (!fs.existsSync(userState.configPath)) {
        const normalized = normalizeConfig(DEFAULT_CONFIG);
        fs.writeFileSync(userState.configPath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf8');
        return normalized;
    }
    try {
        return normalizeConfig(JSON.parse(fs.readFileSync(userState.configPath, 'utf8')));
    } catch (e) {
        const normalized = normalizeConfig(DEFAULT_CONFIG);
        fs.writeFileSync(userState.configPath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf8');
        return normalized;
    }
}

function writeUserConfig(userState, input) {
    const normalized = normalizeConfig(input);
    fs.mkdirSync(userState.dataDir, { recursive: true });
    fs.writeFileSync(userState.configPath, `${JSON.stringify(normalized, null, 2)}\n`, 'utf8');
    return normalized;
}

function updateUserCodeStatus(userState, patch = {}, { emit = true } = {}) {
    const current = readUserConfig(userState);
    const nextInput = { ...current, ...patch };
    const next = normalizeConfig(nextInput);

    const same =
        current.codeStatus === next.codeStatus
        && current.codeStatusReason === next.codeStatusReason
        && current.codeStatusMessage === next.codeStatusMessage
        && current.codeUpdatedAt === next.codeUpdatedAt
        && current.codeLastUsedAt === next.codeLastUsedAt
        && current.codeLastErrorAt === next.codeLastErrorAt
        && current.clientVersion === next.clientVersion
        && current.code === next.code;

    if (same) return current;

    writeUserConfig(userState, next);
    if (emit) {
        broadcastToUser(userState, 'config', buildClientConfig(next));
    }
    return next;
}

function readUserShareContent(userState) {
    if (!fs.existsSync(userState.sharePath)) return '';
    return fs.readFileSync(userState.sharePath, 'utf8');
}

function writeUserShareContent(userState, content) {
    fs.mkdirSync(userState.dataDir, { recursive: true });
    const text = String(content || '');
    fs.writeFileSync(userState.sharePath, text, 'utf8');
    return text;
}

function readUserLandsSnapshot(userState) {
    if (!fs.existsSync(userState.landsPath)) {
        return {
            ok: false,
            hasSnapshot: false,
            message: '地块快照尚未生成（等待 bot 完成一次巡田）',
            snapshot: null,
        };
    }
    try {
        const raw = fs.readFileSync(userState.landsPath, 'utf8');
        const parsed = JSON.parse(raw);
        const stat = fs.statSync(userState.landsPath);
        return {
            ok: true,
            hasSnapshot: true,
            updatedAt: stat.mtime.toISOString(),
            snapshot: decorateLandsSnapshot(parsed && typeof parsed === 'object' ? parsed : null),
        };
    } catch (err) {
        return {
            ok: false,
            hasSnapshot: true,
            message: `地块快照读取失败: ${err.message}`,
            snapshot: null,
        };
    }
}

function readUserBagSnapshot(userState) {
    if (!fs.existsSync(userState.bagSnapshotPath)) {
        return {
            ok: false,
            hasSnapshot: false,
            message: '仓库快照尚未生成（点击“打开仓库”会自动刷新）',
            items: [],
            fruits: [],
            superFruits: [],
            seeds: [],
            props: [],
            updatedAt: null,
            updatedAtMs: 0,
        };
    }
    try {
        const raw = JSON.parse(fs.readFileSync(userState.bagSnapshotPath, 'utf8')) || {};
        const stat = fs.statSync(userState.bagSnapshotPath);
        const updatedAt = String(raw.updated_at || stat.mtime.toISOString() || '');
        const updatedAtMs = new Date(updatedAt || stat.mtime.toISOString()).getTime();
        const items = normalizeBagSnapshotList(raw.items);
        const fruits = normalizeBagSnapshotList(raw.fruits);
        const superFruits = normalizeBagSnapshotList(raw.superFruits);
        const seeds = normalizeBagSnapshotList(raw.seeds);
        const props = normalizeBagSnapshotList(raw.props);
        return {
            ok: true,
            hasSnapshot: true,
            message: '',
            items,
            fruits,
            superFruits,
            seeds,
            props,
            updatedAt,
            updatedAtMs: Number.isFinite(updatedAtMs) ? updatedAtMs : 0,
        };
    } catch (err) {
        return {
            ok: false,
            hasSnapshot: false,
            message: `仓库快照读取失败: ${err.message || String(err)}`,
            items: [],
            fruits: [],
            superFruits: [],
            seeds: [],
            props: [],
            updatedAt: null,
            updatedAtMs: 0,
        };
    }
}

function readUserFriendsSnapshot(userState) {
    const friendsPath = path.join(userState.dataDir, 'friends.json');
    if (!fs.existsSync(friendsPath)) {
        return {
            ok: false,
            hasSnapshot: false,
            updatedAt: null,
            friends: [],
        };
    }
    try {
        const raw = JSON.parse(fs.readFileSync(friendsPath, 'utf8')) || {};
        const stat = fs.statSync(friendsPath);
        const rows = Array.isArray(raw.friends) ? raw.friends : [];
        return {
            ok: true,
            hasSnapshot: true,
            updatedAt: (raw.updated_at || stat.mtime.toISOString() || null),
            friends: rows,
        };
    } catch (err) {
        return {
            ok: false,
            hasSnapshot: true,
            updatedAt: null,
            friends: [],
            error: err.message || String(err),
        };
    }
}

function readUserFriendStats(userState) {
    const statsPath = path.join(userState.dataDir, 'friend-stats.json');
    if (!fs.existsSync(statsPath)) {
        return {
            ok: false,
            hasStats: false,
            updatedAt: null,
            totals: null,
            friends: [],
        };
    }
    try {
        const raw = JSON.parse(fs.readFileSync(statsPath, 'utf8')) || {};
        const stat = fs.statSync(statsPath);
        const rowsRaw = raw.friends && typeof raw.friends === 'object' ? Object.values(raw.friends) : [];
        const rows = rowsRaw.filter((item) => item && typeof item === 'object');
        return {
            ok: true,
            hasStats: true,
            updatedAt: raw.updated_at || stat.mtime.toISOString(),
            totals: raw.totals && typeof raw.totals === 'object' ? raw.totals : null,
            friends: rows,
        };
    } catch (err) {
        return {
            ok: false,
            hasStats: false,
            updatedAt: null,
            totals: null,
            friends: [],
            error: err.message || String(err),
        };
    }
}

function detectUserAutoLandsCount(userState) {
    try {
        if (!fs.existsSync(userState.landsPath)) return null;
        const raw = JSON.parse(fs.readFileSync(userState.landsPath, 'utf8')) || {};
        const unlocked = toInt(raw.unlocked_count, 0, 0, 500);
        if (unlocked > 0) {
            return {
                count: unlocked,
                source: 'snapshot_unlocked',
                detectedAt: raw.updated_at || null,
            };
        }
        const total = toInt(raw.total, 0, 0, 500);
        if (total > 0) {
            return {
                count: total,
                source: 'snapshot_total',
                detectedAt: raw.updated_at || null,
            };
        }
    } catch (e) {
        // ignore snapshot parse failure and fall back to metrics/manual
    }
    const metricsCount = toInt(userState.metrics && userState.metrics.landCount, 0, 0, 500);
    if (metricsCount > 0) {
        return {
            count: metricsCount,
            source: 'metrics',
            detectedAt: (userState.metrics && userState.metrics.landCountUpdatedAt) || null,
        };
    }
    return null;
}

// ============ Process Helpers ============

function pidExists(pid) {
    if (!Number.isInteger(pid) || pid <= 0) return false;
    try {
        process.kill(pid, 0);
        return true;
    } catch (err) {
        return err && err.code === 'EPERM';
    }
}

function readLockPid(lockPath) {
    if (!fs.existsSync(lockPath)) return null;
    try {
        const val = parseInt(fs.readFileSync(lockPath, 'utf8').trim(), 10);
        return Number.isInteger(val) && val > 0 ? val : null;
    } catch (e) { return null; }
}

function getProcessElapsedSec(pid) {
    if (!Number.isInteger(pid) || pid <= 0) return null;
    try {
        const out = spawnSync('ps', ['-p', String(pid), '-o', 'etimes='], { encoding: 'utf8' });
        if (out.status !== 0 || !out.stdout) return null;
        const sec = parseInt(String(out.stdout).trim(), 10);
        return Number.isInteger(sec) && sec >= 0 ? sec : null;
    } catch (e) { return null; }
}

// ============ Per-User Runtime ============

function getUserRuntimeUptimeSec(userState) {
    if (!userState.runtime.running || !userState.runtime.startedAt) return 0;
    const startedMs = new Date(userState.runtime.startedAt).getTime();
    if (!Number.isFinite(startedMs)) return 0;
    return Math.max(0, Math.floor((Date.now() - startedMs) / 1000));
}

function buildUserRuntimePayload(userState) {
    return {
        ...userState.runtime,
        uptimeSec: getUserRuntimeUptimeSec(userState),
        botRoot: BOT_ROOT,
        logPath: userState.logPath,
        metrics: userState.metrics,
    };
}

function refreshUserRuntime(userState, emit = true) {
    const previousPid = userState.runtime.pid;
    const previousRunning = userState.runtime.running;

    const lockPid = readLockPid(userState.lockPath);
    let activePid = null;
    if (lockPid && pidExists(lockPid)) {
        activePid = lockPid;
    } else if (userState.runtime.pid && pidExists(userState.runtime.pid)) {
        activePid = userState.runtime.pid;
    }
    const running = Boolean(activePid);

    userState.runtime.running = running;
    userState.runtime.pid = running ? activePid : null;

    if (!running) {
        userState.runtime.startedAt = null;
    } else {
        if (!userState.runtime.startedAt || previousPid !== activePid) {
            userState.runtime.startedAt = new Date().toISOString();
        }
        const elapsedSec = getProcessElapsedSec(activePid);
        if (Number.isInteger(elapsedSec)) {
            const estimatedStart = new Date(Date.now() - elapsedSec * 1000).toISOString();
            const knownUptime = getUserRuntimeUptimeSec(userState);
            if (!userState.runtime.startedAt || previousPid !== activePid || Math.abs(knownUptime - elapsedSec) > 10) {
                userState.runtime.startedAt = estimatedStart;
            }
        }
    }

    const sig = `${running ? 1 : 0}:${userState.runtime.pid || 0}`;
    const sigChanged = sig !== userState.runtimeSig;
    if (sigChanged) userState.runtimeSig = sig;

    if (previousRunning && !running) {
        maybeNotifyUnexpectedDisconnect(userState, userState.runtime.lastError || userState.metrics.lastError || 'bot 进程已停止');
    }

    if (emit && (sigChanged || running || previousRunning)) {
        broadcastToUser(userState, 'runtime', buildUserRuntimePayload(userState));
    }

    return userState.runtime;
}

// ============ FarmCalc ============

function getFarmCalcModule() {
    if (farmCalcModule) return farmCalcModule;
    if (farmCalcModuleError) throw farmCalcModuleError;
    try {
        // eslint-disable-next-line global-require, import/no-dynamic-require
        farmCalcModule = require(FARM_CALC_PATH);
        return farmCalcModule;
    } catch (err) {
        farmCalcModuleError = err;
        throw err;
    }
}

function resolveFarmCalcLands(query = {}, config = DEFAULT_CONFIG, userState) {
    const useAutoLands = toBoolean(query.useAutoLands, Boolean(config.farmCalcUseAutoLands));
    const queryManualLands = toInt(query.manualLands, 0, 0, 500);
    const manualLands = queryManualLands > 0
        ? queryManualLands
        : toInt(config.farmCalcManualLands, DEFAULT_CONFIG.farmCalcManualLands, 1, 500);
    const autoDetected = detectUserAutoLandsCount(userState);
    const autoLands = autoDetected && autoDetected.count > 0 ? autoDetected.count : null;

    let lands = null;
    let source = 'manual';
    if (useAutoLands && autoLands) {
        lands = autoLands;
        source = autoDetected.source || 'auto';
    } else if (manualLands > 0) {
        lands = manualLands;
        source = useAutoLands ? 'manual_fallback' : 'manual';
    } else if (autoLands) {
        lands = autoLands;
        source = autoDetected.source || 'auto';
    }

    if (!lands || lands <= 0) {
        const err = new Error('无法获取地块数量，请在 FarmCalc 面板输入手动地块数');
        err.statusCode = 400;
        throw err;
    }

    return {
        lands,
        source,
        useAutoLands,
        autoLands,
        manualLands,
        autoDetectedAt: (autoDetected && autoDetected.detectedAt) || userState.metrics.landCountUpdatedAt || null,
    };
}

function buildFarmCalcRecommendation(query = {}, userState) {
    const config = readUserConfig(userState);
    const queryLevel = toInt(query.level, 0, 0, 999);
    const metricsLevel = toInt(userState.metrics.level, 0, 0, 999);
    const level = queryLevel > 0 ? queryLevel : metricsLevel;
    if (!level || level <= 0) {
        const err = new Error('当前等级尚未获取，请先运行机器人获取等级');
        err.statusCode = 400;
        throw err;
    }

    const top = toInt(query.top, 8, 1, 30);
    const landsInput = resolveFarmCalcLands(query, config, userState);
    const farmCalc = getFarmCalcModule();
    const recommendation = farmCalc.getPlantingRecommendation(level, landsInput.lands, { top });
    const bestSeedId = Number(
        (recommendation && recommendation.bestNormalFert && recommendation.bestNormalFert.seedId)
        || (recommendation && recommendation.bestNoFert && recommendation.bestNoFert.seedId)
        || 0
    );

    return {
        level: { value: level, source: queryLevel > 0 ? 'manual' : 'metrics' },
        lands: {
            value: landsInput.lands,
            source: landsInput.source,
            useAutoLands: landsInput.useAutoLands,
            autoDetected: landsInput.autoLands,
            autoDetectedAt: landsInput.autoDetectedAt,
            manualInput: landsInput.manualLands,
        },
        recommendation,
        preferredSeedId: Number(config.preferredSeedId || 0),
        recommendedSeedId: bestSeedId > 0 ? bestSeedId : 0,
    };
}

// ============ Log Processing ============

function classifyLine(line) {
    if (line.includes('⚠') || /\bERROR\b/i.test(line)) return 'warn';
    if (/失败|错误|超时|断开|被踢/.test(line)) return 'error';
    return 'info';
}

function detectTag(line) {
    const m = line.match(/^\[[^\]]+\]\s+\[([^\]]+)\]/);
    if (m) return m[1];
    if (line.startsWith('[WEBUI]')) return 'WEBUI';
    return '系统';
}

function extractActionCount(text, keyword) {
    const m = text.match(new RegExp(`${keyword}(\\d+)`));
    return m ? Number(m[1]) : 0;
}

function parseLandCountFromLine(line) {
    if (!line) return null;
    const text = String(line);
    const patterns = [
        /土地数量\s*[:：]?\s*(\d{1,4})/,
        /土地数\s*[:：]?\s*(\d{1,4})/,
        /\blands?\s*count\s*[:=]\s*(\d{1,4})\b/i,
        /\blands?\s*[:=]\s*(\d{1,4})\b/i,
        /AllLands Reply:\s*(\d{1,4})\s*块土地/i,
    ];
    for (const pattern of patterns) {
        const match = text.match(pattern);
        if (!match) continue;
        const count = Number(match[1]);
        if (Number.isInteger(count) && count > 0) return count;
    }
    return null;
}

function syncCodeStatusFromLogLine(userState, line) {
    const text = String(line || '');
    if (!text) return;

    if (/登录成功/.test(text) && userState.runtime && userState.runtime.running) {
        const current = readUserConfig(userState);
        if (current.codeStatus === 'ready') return;
        updateUserCodeStatus(userState, {
            codeStatus: 'active',
            codeStatusReason: '',
            codeStatusMessage: '',
            codeLastUsedAt: new Date().toISOString(),
        });
        return;
    }

    const isKicked =
        /被顶下线/.test(text)
        || /玩家已在其他地方登录/.test(text)
        || /\bcode=1000014\b/.test(text);
    if (isKicked) {
        updateUserCodeStatus(userState, {
            codeStatus: 'error',
            codeStatusReason: 'kicked',
            codeStatusMessage: '账号被顶下线（其他地方登录）',
            codeLastErrorAt: new Date().toISOString(),
        });
        return;
    }

    if (/Unexpected server response:\s*400/.test(text)) {
        updateUserCodeStatus(userState, {
            codeStatus: 'error',
            codeStatusReason: 'expired',
            codeStatusMessage: 'WS 握手失败(400)，code 可能失效或已过期',
            codeLastErrorAt: new Date().toISOString(),
        });
        return;
    }

    const hasLoginCodeError =
        /\bUserService\.Login\b/.test(text)
        && /\bcode=\d+\b/.test(text);
    if (hasLoginCodeError) {
        const isBusyLoginError = /\bcode=1020001\b/.test(text) || /网络繁忙/.test(text);
        updateUserCodeStatus(userState, {
            codeStatus: 'error',
            codeStatusReason: isBusyLoginError ? 'login_failed' : 'expired',
            codeStatusMessage: isBusyLoginError
                ? '登录被拒绝：网络繁忙。当前 code 这轮已不可复用，请换新 code 再试'
                : '登录 code 失效或已过期，请更新',
            codeLastErrorAt: new Date().toISOString(),
        });
    }
}

function syncRuntimeConfigFromLogLine(userState, line) {
    const text = String(line || '');
    if (!text) return;

    if (/friendActionCare auto-disabled \(care exp exhausted\)/.test(text)) {
        try {
            broadcastToUser(userState, 'config', buildClientConfig(readUserConfig(userState)));
        } catch (e) {
            // ignore
        }
    }

    if (/autoFertilize auto-disabled \(normal fertilizer low/.test(text)) {
        try {
            broadcastToUser(userState, 'config', buildClientConfig(readUserConfig(userState)));
        } catch (e) {
            // ignore
        }
    }
}

function parseMetricsFromLine(userState, line) {
    const metrics = userState.metrics;
    syncCodeStatusFromLogLine(userState, line);
    syncRuntimeConfigFromLogLine(userState, line);

    const fertSecMatch = line.match(/\bFERT_STATUS\b.*\bnormal_sec=(\d+)\b/i);
    const fertCountMatch = line.match(/\bFERT_STATUS\b.*\bnormal_count=(\d+)\b/i);
    if (fertSecMatch) {
        const count = Number(fertSecMatch[1]);
        if (Number.isFinite(count) && count >= 0) {
            metrics.normalFertilizerCount = count;
            metrics.normalFertilizerState = count > 0 ? 'ok' : 'low';
            metrics.normalFertilizerUpdatedAt = new Date().toISOString();
        }
    } else if (fertCountMatch) {
        const count = Number(fertCountMatch[1]);
        if (Number.isFinite(count) && count >= 0) {
            metrics.normalFertilizerCount = count;
            metrics.normalFertilizerState = count > 0 ? 'ok' : 'low';
            metrics.normalFertilizerUpdatedAt = new Date().toISOString();
        }
    } else if (/\bFERT_STATUS\s+ok\b/i.test(line)) {
        metrics.normalFertilizerCount = null;
        metrics.normalFertilizerState = 'ok';
        metrics.normalFertilizerUpdatedAt = new Date().toISOString();
    } else if (/\bFERT_STATUS\s+low\b/i.test(line)) {
        const lowCountMatch = line.match(/\bnormal_count=(\d+)\b/i);
        const lowCount = lowCountMatch ? Number(lowCountMatch[1]) : null;
        metrics.normalFertilizerCount = Number.isFinite(lowCount) ? lowCount : null;
        metrics.normalFertilizerState = 'low';
        metrics.normalFertilizerUpdatedAt = new Date().toISOString();
    }

    if (line.includes('⚠') || /失败|错误|超时/.test(line)) {
        metrics.errorCount += 1;
        metrics.lastError = line;
        userState.runtime.lastError = line;
    }

    const farmSummaryMatch = line.match(/\[农场\]\s+\[([^\]]+)\]/);
    if (farmSummaryMatch) {
        const parts = farmSummaryMatch[1].trim().split(/\s+/);
        for (const part of parts) {
            const m = part.match(/(收|草|虫|水):(\d+)/);
            if (!m) continue;
            const count = Number(m[2]);
            if (m[1] === '收') metrics.harvest += count;
            if (m[1] === '草') metrics.weed += count;
            if (m[1] === '虫') metrics.bug += count;
            if (m[1] === '水') metrics.water += count;
        }
    }

    const friendSummaryMatch = line.match(/\[好友\]\s+巡查\s+\d+\s+人\s+→\s+(.+)$/);
    if (friendSummaryMatch) {
        const summary = friendSummaryMatch[1];
        metrics.steal += extractActionCount(summary, '偷');
        metrics.weed += extractActionCount(summary, '除草');
        metrics.bug += extractActionCount(summary, '除虫');
        metrics.water += extractActionCount(summary, '浇水');
    }

    const taskMatch = line.match(/\[(?:任务|浏览奖励)\]\s+(?:领取|获得)[:：]?\s*(.+)$/);
    if (taskMatch) {
        metrics.taskClaims += 1;
        metrics.lastTask = taskMatch[1];
    }

    if (line.includes('[仓库] 出售')) {
        metrics.soldBatches += 1;
        const sold = [...line.matchAll(/x(\d+)/g)].reduce((acc, m) => acc + Number(m[1]), 0);
        metrics.soldItems += sold;
    }

    const profileMatch = line.match(/\[仓库\]\s+总金币\s+(\d+)(?:\s+\|\s+点券\s+(\d+))?\s+\|\s+Lv(\d+)(?:\s+经验\s+(\d+)\/(\d+))?/);
    if (profileMatch) {
        metrics.gold = Number(profileMatch[1]);
        if (profileMatch[2]) metrics.diamond = Number(profileMatch[2]);
        metrics.level = Number(profileMatch[3]);
        if (profileMatch[4] && profileMatch[5]) {
            metrics.expCurrent = Number(profileMatch[4]);
            metrics.expNeeded = Number(profileMatch[5]);
        }
    }

    const loginLevelMatch = line.match(/^\s*等级:\s*(\d+)/);
    if (loginLevelMatch) metrics.level = Number(loginLevelMatch[1]);

    const loginGoldMatch = line.match(/^\s*金币:\s*(\d+)/);
    if (loginGoldMatch) metrics.gold = Number(loginGoldMatch[1]);
    const loginDiamondMatch = line.match(/^\s*点券:\s*(\d+)/);
    if (loginDiamondMatch) metrics.diamond = Number(loginDiamondMatch[1]);

    const shopLevelMatch = line.match(/等级\s*[:：]?\s*(\d+)/);
    if (shopLevelMatch) metrics.level = Number(shopLevelMatch[1]);

    const landsCount = parseLandCountFromLine(line);
    if (landsCount) {
        metrics.landCount = landsCount;
        metrics.landCountUpdatedAt = new Date().toISOString();
    }

    metrics.lastUpdateAt = new Date().toISOString();
}

function getWarehouseProfileSignature(line) {
    if (!line) return '';
    const m = String(line).match(/\[仓库\]\s+总金币\s+(\d+)(?:\s+\|\s+点券\s+(\d+))?\s+\|\s+Lv(\d+)(?:\s+经验\s+(\d+)\/(\d+))?/);
    if (!m) return '';
    return `${m[1] || ''}:${m[2] || ''}:${m[3] || ''}:${m[4] || ''}:${m[5] || ''}`;
}

function shouldSuppressProfileLog(userState, line) {
    const signature = getWarehouseProfileSignature(line);
    if (!signature) return false;
    const now = Date.now();
    const sameAsLast = userState.profileLogEcho.signature === signature;
    const inWindow = (now - userState.profileLogEcho.at) < PROFILE_LOG_REPEAT_WINDOW_MS;
    if (sameAsLast && inWindow) return true;
    userState.profileLogEcho.signature = signature;
    userState.profileLogEcho.at = now;
    return false;
}

function addUserLogLine(userState, line, emit = true) {
    const text = String(line || '').trimEnd();
    if (!text) return;

    parseMetricsFromLine(userState, text);
    if (shouldSuppressProfileLog(userState, text)) {
        if (emit) broadcastToUser(userState, 'metrics', userState.metrics);
        return;
    }

    const entry = {
        id: userState.nextLogId++,
        time: new Date().toISOString(),
        tag: detectTag(text),
        level: classifyLine(text),
        text,
    };

    userState.logs.push(entry);
    if (userState.logs.length > MAX_LOG_LINES) {
        userState.logs.splice(0, userState.logs.length - MAX_LOG_LINES);
    }

    if (emit) {
        broadcastToUser(userState, 'log', entry);
        broadcastToUser(userState, 'metrics', userState.metrics);
    }
}

function addUserLogText(userState, chunk, emit = true) {
    const content = `${userState.logRemainder}${chunk || ''}`;
    const lines = content.split(/\r?\n/);
    userState.logRemainder = lines.pop() || '';
    for (const line of lines) addUserLogLine(userState, line, emit);
}

function loadUserInitialLogTail(userState) {
    userState.logOffset = 0;
    userState.logRemainder = '';
    const logPath = userState.logPath;
    if (!fs.existsSync(logPath)) return;
    const stat = fs.statSync(logPath);
    const size = stat.size;
    if (size <= 0) { userState.logOffset = 0; return; }

    const tailBytes = Math.min(size, 300 * 1024);
    const start = size - tailBytes;
    const fd = fs.openSync(logPath, 'r');
    try {
        const buf = Buffer.alloc(tailBytes);
        fs.readSync(fd, buf, 0, tailBytes, start);
        let text = buf.toString('utf8');
        if (start > 0) {
            const firstNewline = text.indexOf('\n');
            text = firstNewline >= 0 ? text.slice(firstNewline + 1) : '';
        }
        addUserLogText(userState, text, false);
        userState.logRemainder = '';
        userState.logOffset = size;
    } finally {
        fs.closeSync(fd);
    }
}

function pollUserLogAppend(userState) {
    const logPath = userState.logPath;
    if (!fs.existsSync(logPath)) {
        userState.logOffset = 0;
        userState.logRemainder = '';
        return;
    }
    const stat = fs.statSync(logPath);
    if (stat.size < userState.logOffset) {
        userState.logOffset = 0;
        userState.logRemainder = '';
    }
    if (stat.size === userState.logOffset) return;

    const fd = fs.openSync(logPath, 'r');
    try {
        let position = userState.logOffset;
        let remaining = stat.size - userState.logOffset;
        while (remaining > 0) {
            const size = Math.min(remaining, MAX_READ_CHUNK);
            const buf = Buffer.alloc(size);
            const bytesRead = fs.readSync(fd, buf, 0, size, position);
            if (bytesRead <= 0) break;
            position += bytesRead;
            remaining -= bytesRead;
            addUserLogText(userState, buf.subarray(0, bytesRead).toString('utf8'), true);
        }
        userState.logOffset = stat.size;
    } finally {
        fs.closeSync(fd);
    }
}

// ============ SSE ============

function sseWrite(res, event, payload) {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcastToUser(userState, event, payload) {
    for (const client of userState.clients) {
        sseWrite(client, event, payload);
    }
}

// ============ Bot Process Management ============

function splitExtraArgs(extraArgs) {
    const result = [];
    const re = /"([^"]*)"|'([^']*)'|(\S+)/g;
    let match;
    while ((match = re.exec(extraArgs)) !== null) {
        result.push(match[1] ?? match[2] ?? match[3]);
    }
    return result;
}

function startUserBotDirect(userState, config) {
    // clean stale lock
    const lockPid = readLockPid(userState.lockPath);
    if (lockPid && !pidExists(lockPid)) {
        try { fs.unlinkSync(userState.lockPath); } catch (e) { }
    }

    fs.mkdirSync(path.dirname(userState.logPath), { recursive: true });
    fs.appendFileSync(userState.logPath, `==== ${new Date().toISOString()} webui start (user=${userState.username}) ====\n`, 'utf8');

    const args = [
        path.join(BOT_ROOT, 'client.js'),
        '--code',
        config.code,
        '--client-version',
        config.clientVersion,
    ];
    if (config.platform === 'wx') args.push('--wx');
    if (Number.isFinite(Number(config.intervalSec))) args.push('--interval', String(config.intervalSec));
    if (Number.isFinite(Number(config.friendIntervalSec))) args.push('--friend-interval', String(config.friendIntervalSec));
    if (config.preferredSeedId > 0) args.push('--seed-id', String(config.preferredSeedId));
    if (config.allowMulti) args.push('--allow-multi');
    if (config.extraArgs) args.push(...splitExtraArgs(config.extraArgs));

    const outFd = fs.openSync(userState.logPath, 'a');
    const errFd = fs.openSync(userState.logPath, 'a');
    const child = spawn(process.execPath, args, {
        cwd: BOT_ROOT,
        detached: true,
        stdio: ['ignore', outFd, errFd],
        env: { ...process.env, QQ_FARM_DATA_DIR: userState.dataDir },
    });
    child.unref();
    try { fs.closeSync(outFd); } catch (e) { }
    try { fs.closeSync(errFd); } catch (e) { }

    return { pid: child.pid };
}

async function startUserBot(userState) {
    const config = readUserConfig(userState);
    if (!config.code || !config.clientVersion) {
        const err = new Error('请先填写登录 code 和版本号');
        err.statusCode = 400;
        throw err;
    }

    refreshUserRuntime(userState, false);
    if (userState.runtime.running) {
        return { message: '机器人已在运行', runtime: buildUserRuntimePayload(userState) };
    }

    const direct = startUserBotDirect(userState, config);
    userState.runtime.pid = direct.pid;
    userState.runtime.running = true;
    userState.runtime.startedAt = new Date().toISOString();
    userState.runtime.lastAction = 'start';
    userState.runtime.lastOutput = `已启动 pid=${direct.pid}`;
    userState.runtime.lastError = '';
    if (config.code) {
        updateUserCodeStatus(userState, {
            codeStatus: 'active',
            codeStatusReason: '',
            codeStatusMessage: '',
            codeLastUsedAt: new Date().toISOString(),
        });
    }

    addUserLogLine(userState, `[WEBUI] 已启动机器人 pid=${direct.pid} 平台=${config.platform} ver=${config.clientVersion} 农场间隔=${config.intervalSec}s 好友间隔=${config.friendIntervalSec}s`, true);

    const deadline = Date.now() + 10000;
    while (!userState.runtime.running && Date.now() < deadline) {
        await delay(500);
        refreshUserRuntime(userState, false);
    }

    return {
        message: '启动命令已执行',
        runtime: buildUserRuntimePayload(userState),
        output: userState.runtime.lastOutput,
    };
}

async function stopUserBot(userState) {
    refreshUserRuntime(userState, false);
    const pid = userState.runtime.pid;

    if (pid && pidExists(pid)) {
        try { process.kill(pid, 'SIGTERM'); } catch (e) { }
        await delay(800);
        if (pidExists(pid)) {
            try { process.kill(pid, 'SIGKILL'); } catch (e) { }
        }
    }

    try { if (fs.existsSync(userState.lockPath)) fs.unlinkSync(userState.lockPath); } catch (e) { }
    userState.runtime.running = false;
    userState.runtime.pid = null;
    userState.runtime.startedAt = null;
    userState.runtime.lastAction = 'stop';
    userState.runtime.lastOutput = '已发送停止信号';

    addUserLogLine(userState, '[WEBUI] 已执行停止操作', true);
    await delay(300);

    return { message: '停止命令已执行', runtime: buildUserRuntimePayload(userState) };
}

// ============ Plant Options ============

function readPlantOptions() {
    if (!fs.existsSync(SEED_SHOP_DATA_PATH)) return [];
    try {
        const parsed = JSON.parse(fs.readFileSync(SEED_SHOP_DATA_PATH, 'utf8'));
        const rows = Array.isArray(parsed) ? parsed : (parsed.rows || parsed.seeds || []);
        if (!Array.isArray(rows)) return [];

        const options = [];
        const seedIdSet = new Set();
        for (const item of rows) {
            if (!item || typeof item !== 'object') continue;
            const seedId = Number(item.seedId || item.seed_id);
            if (!Number.isInteger(seedId) || seedId <= 0 || seedIdSet.has(seedId)) continue;
            seedIdSet.add(seedId);
            const name = String(item.name || '').trim() || `seed-${seedId}`;
            const levelNeed = toInt(item.requiredLevel || item.required_level, 0, 0, 9999);
            const exp = toInt(item.exp, 0, 0, 999999);
            options.push({ seedId, name, levelNeed, exp });
        }
        options.sort((a, b) => (a.levelNeed - b.levelNeed) || (a.seedId - b.seedId));
        return options;
    } catch (e) { return []; }
}

function normalizeCropName(name) {
    return String(name || '')
        .replace(/\s+/g, '')
        .trim();
}

function parseStealPlantNames(raw) {
    const text = String(raw || '').trim();
    if (!text) return [];
    const parts = text
        .split(/[\/、,，]/)
        .map((item) => item.trim())
        .filter(Boolean);
    return Array.from(new Set(parts));
}

function roundTo(n, digits = 2) {
    const p = 10 ** digits;
    const v = Number(n);
    if (!Number.isFinite(v)) return 0;
    return Math.round(v * p) / p;
}

function toTimestamp(value) {
    const t = Date.parse(value || '');
    return Number.isFinite(t) ? t : 0;
}

function updateLatestTime(current, next) {
    if (!next) return current || null;
    if (!current) return next;
    return toTimestamp(next) >= toTimestamp(current) ? next : current;
}

function readSeedUnitValueMap() {
    if (!fs.existsSync(SEED_SHOP_DATA_PATH)) return new Map();
    try {
        const stat = fs.statSync(SEED_SHOP_DATA_PATH);
        const mtimeMs = Number(stat.mtimeMs || 0);
        if (seedUnitValueCache.map && seedUnitValueCache.mtimeMs === mtimeMs) {
            return seedUnitValueCache.map;
        }

        const map = new Map();
        const parsed = JSON.parse(fs.readFileSync(SEED_SHOP_DATA_PATH, 'utf8'));
        const rows = Array.isArray(parsed) ? parsed : (parsed.rows || parsed.seeds || []);
        if (!Array.isArray(rows)) return map;
        for (const row of rows) {
            if (!row || typeof row !== 'object') continue;
            const key = normalizeCropName(row.name);
            if (!key) continue;
            const price = Number(row.price || 0);
            if (!Number.isFinite(price) || price <= 0) continue;
            const prev = Number(map.get(key) || 0);
            if (!prev || price > prev) {
                map.set(key, price);
            }
        }
        seedUnitValueCache = { mtimeMs, map };
        return map;
    } catch (e) {
        // ignore and keep old cache
    }
    return seedUnitValueCache.map || new Map();
}

function buildSaleUnitValueMap(logEntries) {
    const agg = new Map();
    const entries = Array.isArray(logEntries) ? logEntries : [];
    for (const entry of entries) {
        const line = String(entry && entry.text ? entry.text : '');
        if (!line || !line.includes('[仓库]') || !line.includes('出售')) continue;
        const saleMatch = line.match(/\[仓库\]\s+出售\s+(.+?)[，,]\s*获得\s*(\d+)\s*金币/);
        if (!saleMatch) continue;

        const goodsPart = saleMatch[1];
        const totalGold = Number(saleMatch[2]);
        if (!Number.isFinite(totalGold) || totalGold <= 0) continue;

        const items = [...goodsPart.matchAll(/([^，,]+?)x(\d+)/g)];
        if (items.length !== 1) continue;

        const cropName = normalizeCropName(items[0][1]);
        const qty = Number(items[0][2]);
        if (!cropName || !Number.isFinite(qty) || qty <= 0) continue;

        const prev = agg.get(cropName) || { gold: 0, qty: 0 };
        prev.gold += totalGold;
        prev.qty += qty;
        agg.set(cropName, prev);
    }

    const unitMap = new Map();
    for (const [name, data] of agg.entries()) {
        if (!data || data.qty <= 0 || data.gold <= 0) continue;
        unitMap.set(name, data.gold / data.qty);
    }
    return unitMap;
}

function pad2(n) {
    return String(Number(n) || 0).padStart(2, '0');
}

function formatLocalDayKey(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
    return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function formatLocalDayLabel(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '--';
    return `${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function floorToLocalHour(date) {
    const d = new Date(date);
    d.setMinutes(0, 0, 0);
    return d;
}

function parseLogLineTimestamp(line, sessionDate, previousDate) {
    if (!sessionDate) return null;
    const match = String(line || '').match(/^\[(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)\]/i);
    if (!match) return null;

    let hour = Number(match[1]);
    const minute = Number(match[2]);
    const second = Number(match[3]);
    const meridiem = String(match[4] || '').toUpperCase();

    if (meridiem === 'PM' && hour < 12) hour += 12;
    if (meridiem === 'AM' && hour === 12) hour = 0;

    const anchor = previousDate instanceof Date && !Number.isNaN(previousDate.getTime())
        ? new Date(previousDate)
        : new Date(sessionDate);
    const candidate = new Date(anchor);
    candidate.setHours(hour, minute, second, 0);

    if (previousDate instanceof Date && !Number.isNaN(previousDate.getTime())) {
        const backwardMs = previousDate.getTime() - candidate.getTime();
        if (backwardMs > 6 * 3600 * 1000) {
            candidate.setDate(candidate.getDate() + 1);
        }
    } else {
        const firstBackwardMs = sessionDate.getTime() - candidate.getTime();
        if (firstBackwardMs > 6 * 3600 * 1000) {
            candidate.setDate(candidate.getDate() + 1);
        }
    }

    return candidate;
}

function readParsedLogTimeline(userState) {
    const logPath = userState.logPath;
    if (!fs.existsSync(logPath)) {
        return {
            entries: [],
            updatedAt: null,
            generatedAt: new Date().toISOString(),
        };
    }

    try {
        const stat = fs.statSync(logPath);
        const cacheKey = `${logPath}:${Number(stat.size || 0)}:${Number(stat.mtimeMs || 0)}`;
        const cached = overviewStatsCache.get(cacheKey);
        if (cached) return cached;

        for (const key of overviewStatsCache.keys()) {
            if (String(key).startsWith(`${logPath}:`)) {
                overviewStatsCache.delete(key);
            }
        }

        const raw = fs.readFileSync(logPath, 'utf8');
        const lines = raw.split(/\r?\n/);
        const entries = [];
        let sessionDate = null;
        let previousDate = null;

        for (const rawLine of lines) {
            const line = String(rawLine || '');
            if (!line) continue;

            const sessionMatch = line.match(/^====\s+([0-9T:\-.]+Z)\s+webui start\b/i);
            if (sessionMatch) {
                const sessionValue = new Date(sessionMatch[1]);
                sessionDate = Number.isNaN(sessionValue.getTime()) ? null : sessionValue;
                previousDate = sessionDate;
                continue;
            }

            const parsedDate = parseLogLineTimestamp(line, sessionDate, previousDate);
            if (!parsedDate) continue;
            previousDate = parsedDate;
            entries.push({
                time: parsedDate.toISOString(),
                atMs: parsedDate.getTime(),
                text: line,
            });
        }

        const payload = {
            entries,
            updatedAt: stat.mtime.toISOString(),
            generatedAt: new Date().toISOString(),
        };
        overviewStatsCache.set(cacheKey, payload);
        return payload;
    } catch (err) {
        return {
            entries: [],
            updatedAt: null,
            generatedAt: new Date().toISOString(),
            error: err.message || String(err),
        };
    }
}

function createEmptyStatsBucket(base) {
    return {
        ...base,
        saleGold: 0,
        stealGold: 0,
        stealCount: 0,
        harvestCount: 0,
        taskClaims: 0,
        soldItems: 0,
    };
}

function buildOverviewStatsPayload(userState) {
    const timeline = readParsedLogTimeline(userState);
    const entries = Array.isArray(timeline.entries) ? timeline.entries : [];
    const unitSaleMap = buildSaleUnitValueMap(entries);
    const unitSeedMap = readSeedUnitValueMap();

    const latestEntryAt = entries.length > 0 ? Number(entries[entries.length - 1].atMs || 0) : 0;
    const anchorDate = latestEntryAt > 0 ? new Date(latestEntryAt) : new Date();
    const anchorHour = floorToLocalHour(anchorDate);

    const hourly = [];
    const hourlyIndex = new Map();
    for (let idx = 23; idx >= 0; idx -= 1) {
        const hourDate = new Date(anchorHour);
        hourDate.setHours(hourDate.getHours() - idx);
        const hourKey = hourDate.toISOString();
        const bucket = createEmptyStatsBucket({
            hour: hourKey,
            hourLabel: `${pad2(hourDate.getHours())}:00`,
        });
        hourly.push(bucket);
        hourlyIndex.set(hourKey, bucket);
    }

    const daily = [];
    const dailyIndex = new Map();
    const anchorDay = new Date(anchorDate);
    anchorDay.setHours(0, 0, 0, 0);
    for (let idx = 6; idx >= 0; idx -= 1) {
        const dayDate = new Date(anchorDay);
        dayDate.setDate(dayDate.getDate() - idx);
        const dayKey = formatLocalDayKey(dayDate);
        const bucket = createEmptyStatsBucket({
            day: dayKey,
            dayLabel: formatLocalDayLabel(dayDate),
        });
        daily.push(bucket);
        dailyIndex.set(dayKey, bucket);
    }

    const earliestHourMs = hourly.length > 0 ? Date.parse(hourly[0].hour) : 0;
    const earliestDayKey = daily.length > 0 ? daily[0].day : '';

    const applyToBucket = (bucket, patch) => {
        if (!bucket || !patch) return;
        bucket.saleGold += Number(patch.saleGold || 0);
        bucket.stealGold += Number(patch.stealGold || 0);
        bucket.stealCount += Number(patch.stealCount || 0);
        bucket.harvestCount += Number(patch.harvestCount || 0);
        bucket.taskClaims += Number(patch.taskClaims || 0);
        bucket.soldItems += Number(patch.soldItems || 0);
    };

    for (const entry of entries) {
        if (!entry || !entry.text || !Number.isFinite(entry.atMs)) continue;
        if (earliestHourMs > 0 && entry.atMs < earliestHourMs) {
            const dayKey = formatLocalDayKey(new Date(entry.atMs));
            if (!dayKey || (earliestDayKey && dayKey < earliestDayKey)) continue;
        }

        const line = String(entry.text || '');
        const bucketPatch = {
            saleGold: 0,
            stealGold: 0,
            stealCount: 0,
            harvestCount: 0,
            taskClaims: 0,
            soldItems: 0,
        };

        const saleMatch = line.match(/\[仓库\]\s+出售\s+(.+?)[，,]\s*获得\s*(\d+)\s*金币/);
        if (saleMatch) {
            bucketPatch.saleGold += Number(saleMatch[2] || 0);
            bucketPatch.soldItems += [...saleMatch[1].matchAll(/x(\d+)/g)]
                .reduce((sum, item) => sum + Number(item[1] || 0), 0);
        }

        const harvestSummaryMatch = line.match(/\[农场\]\s+\[([^\]]+)\]/);
        if (harvestSummaryMatch) {
            const parts = String(harvestSummaryMatch[1] || '').trim().split(/\s+/);
            for (const part of parts) {
                const match = part.match(/收:(\d+)/);
                if (!match) continue;
                bucketPatch.harvestCount += Number(match[1] || 0);
            }
        }

        if (/\[(?:任务|浏览奖励)\]\s+(?:领取|获得)/.test(line)) {
            bucketPatch.taskClaims += 1;
        }

        const stealMatch = line.match(/(?:偷菜|steal)\s*(\d+)\s*\(([^)]+)\)/i);
        if (stealMatch) {
            const stealCount = Number(stealMatch[1] || 0);
            const plantNames = parseStealPlantNames(stealMatch[2] || '');
            bucketPatch.stealCount += stealCount;
            if (stealCount > 0 && plantNames.length > 0) {
                const unitValues = [];
                for (const plantName of plantNames) {
                    const cropKey = normalizeCropName(plantName);
                    if (!cropKey) continue;
                    const unitValue = Number(unitSaleMap.get(cropKey) || unitSeedMap.get(cropKey) || 0);
                    if (Number.isFinite(unitValue) && unitValue > 0) {
                        unitValues.push(unitValue);
                    }
                }
                if (unitValues.length > 0) {
                    const avgValue = unitValues.reduce((sum, value) => sum + value, 0) / unitValues.length;
                    bucketPatch.stealGold += stealCount * avgValue;
                }
            }
        }

        const hourBucket = hourlyIndex.get(floorToLocalHour(new Date(entry.atMs)).toISOString());
        const dayBucket = dailyIndex.get(formatLocalDayKey(new Date(entry.atMs)));
        applyToBucket(hourBucket, bucketPatch);
        applyToBucket(dayBucket, bucketPatch);
    }

    const summary24h = hourly.reduce((acc, row) => ({
        saleGold: acc.saleGold + row.saleGold,
        stealGold: acc.stealGold + row.stealGold,
        stealCount: acc.stealCount + row.stealCount,
        harvestCount: acc.harvestCount + row.harvestCount,
        taskClaims: acc.taskClaims + row.taskClaims,
        soldItems: acc.soldItems + row.soldItems,
    }), createEmptyStatsBucket({}));

    const normalizeBucket = (bucket) => ({
        ...bucket,
        saleGold: roundTo(bucket.saleGold, 2),
        stealGold: roundTo(bucket.stealGold, 2),
        stealCount: roundTo(bucket.stealCount, 1),
        harvestCount: roundTo(bucket.harvestCount, 1),
        taskClaims: roundTo(bucket.taskClaims, 1),
        soldItems: roundTo(bucket.soldItems, 1),
    });

    return {
        ok: true,
        generatedAt: new Date().toISOString(),
        source: {
            updatedAt: timeline.updatedAt || null,
            logEntries: entries.length,
            note: '按日志聚合：出售收益来自仓库出售记录，偷菜收益为按作物单价估算。',
        },
        summary24h: normalizeBucket(summary24h),
        hourly: hourly.map(normalizeBucket),
        daily: daily.map(normalizeBucket),
    };
}

function formatNotifyTime(dateLike = new Date()) {
    const date = dateLike instanceof Date ? dateLike : new Date(dateLike);
    if (Number.isNaN(date.getTime())) return '--';
    return date.toLocaleString('zh-CN', { hour12: false });
}

function getAllKnownUserStates() {
    const users = loadUsers();
    const usernames = Array.from(new Set(users.map((item) => String(item && item.username || '').trim().toLowerCase()).filter(Boolean)));
    return usernames.map((username) => getUserState(username));
}

function summarizeOverviewWindow(overviewPayload, type) {
    if (!overviewPayload || typeof overviewPayload !== 'object') {
        return {
            saleGold: 0,
            stealGold: 0,
            harvestCount: 0,
            stealCount: 0,
            taskClaims: 0,
            soldItems: 0,
        };
    }
    if (type === 'hourly') {
        const rows = Array.isArray(overviewPayload.hourly) ? overviewPayload.hourly : [];
        return rows.length > 0 ? rows[rows.length - 1] : {
            saleGold: 0,
            stealGold: 0,
            harvestCount: 0,
            stealCount: 0,
            taskClaims: 0,
            soldItems: 0,
        };
    }
    return overviewPayload.summary24h || {
        saleGold: 0,
        stealGold: 0,
        harvestCount: 0,
        stealCount: 0,
        taskClaims: 0,
        soldItems: 0,
    };
}

function buildReportAccountEntry(userState, type) {
    if (!Array.isArray(userState.logs) || userState.logs.length <= 0) {
        loadUserInitialLogTail(userState);
    }
    refreshUserRuntime(userState, false);
    const overview = buildOverviewStatsPayload(userState);
    const summary = summarizeOverviewWindow(overview, type);
    const nickname = userState.metrics.level !== null
        ? `${userState.username}`
        : userState.username;
    return {
        username: userState.username,
        nickname,
        running: Boolean(userState.runtime.running),
        uptimeSec: getUserRuntimeUptimeSec(userState),
        level: Number(userState.metrics.level || 0),
        gold: Number(userState.metrics.gold || 0),
        diamond: Number(userState.metrics.diamond || 0),
        errorCount: Number(userState.metrics.errorCount || 0),
        lastError: String(userState.runtime.lastError || userState.metrics.lastError || '').trim(),
        summary: {
            saleGold: Number(summary.saleGold || 0),
            stealGold: Number(summary.stealGold || 0),
            harvestCount: Number(summary.harvestCount || 0),
            stealCount: Number(summary.stealCount || 0),
            taskClaims: Number(summary.taskClaims || 0),
            soldItems: Number(summary.soldItems || 0),
        },
    };
}

function collectReportPayload(type, usersInput = null) {
    const users = Array.isArray(usersInput) && usersInput.length > 0 ? usersInput : getAllKnownUserStates();
    const accounts = users.map((userState) => buildReportAccountEntry(userState, type));
    const totals = accounts.reduce((acc, item) => {
        acc.saleGold += item.summary.saleGold;
        acc.stealGold += item.summary.stealGold;
        acc.harvestCount += item.summary.harvestCount;
        acc.stealCount += item.summary.stealCount;
        acc.taskClaims += item.summary.taskClaims;
        acc.soldItems += item.summary.soldItems;
        return acc;
    }, {
        saleGold: 0,
        stealGold: 0,
        harvestCount: 0,
        stealCount: 0,
        taskClaims: 0,
        soldItems: 0,
    });
    return { accounts, totals };
}

function buildReportMarkdown(type, payload, now = new Date()) {
    const label = type === 'daily' ? '每日汇报' : '每小时汇报';
    let text = `## QQ农场 ${label}\n\n`;
    text += `时间：${formatNotifyTime(now)}\n\n`;
    text += `### 账号汇总\n`;
    text += `- 出售收益：${formatNumberSafe(payload.totals.saleGold)}\n`;
    text += `- 偷菜估值：${formatNumberSafe(payload.totals.stealGold)}\n`;
    text += `- 收获次数：${formatNumberSafe(payload.totals.harvestCount)}\n`;
    text += `- 偷菜次数：${formatNumberSafe(payload.totals.stealCount)}\n`;
    text += `- 任务领取：${formatNumberSafe(payload.totals.taskClaims)}\n`;
    text += `- 出售果实数：${formatNumberSafe(payload.totals.soldItems)}\n\n`;
    for (const account of payload.accounts) {
        text += `### ${account.running ? '运行中' : '离线'} · ${account.nickname}\n`;
        text += `- 等级：Lv${account.level || 0} | 金币：${formatNumberSafe(account.gold)} | 点券：${formatNumberSafe(account.diamond)}\n`;
        text += `- 出售收益：${formatNumberSafe(account.summary.saleGold)} | 偷菜估值：${formatNumberSafe(account.summary.stealGold)}\n`;
        text += `- 收获：${formatNumberSafe(account.summary.harvestCount)} | 偷菜：${formatNumberSafe(account.summary.stealCount)} | 任务：${formatNumberSafe(account.summary.taskClaims)}\n`;
        if (account.lastError) {
            text += `- 最近错误：${account.lastError}\n`;
        }
        text += '\n';
    }
    return text.trim();
}

function buildReportHtml(type, payload, now = new Date()) {
    const label = type === 'daily' ? '每日汇报' : '每小时汇报';
    const accountCards = payload.accounts.map((account) => `
        <div style="border:1px solid #e4e8f1;border-radius:14px;padding:16px 18px;margin-top:12px;background:#ffffff;">
            <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;">
                <strong style="font-size:16px;color:#1f2a37;">${escapeHtml(account.nickname)}</strong>
                <span style="font-size:12px;padding:4px 10px;border-radius:999px;background:${account.running ? '#e8fff1' : '#fff1f2'};color:${account.running ? '#16784d' : '#9f1239'};">${account.running ? '运行中' : '离线'}</span>
            </div>
            <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:12px;font-size:13px;color:#4b5563;">
                <div>等级：<strong style="color:#111827;">Lv${account.level || 0}</strong></div>
                <div>金币：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.gold))}</strong></div>
                <div>点券：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.diamond))}</strong></div>
                <div>错误数：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.errorCount))}</strong></div>
                <div>出售收益：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.summary.saleGold))}</strong></div>
                <div>偷菜估值：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.summary.stealGold))}</strong></div>
                <div>收获次数：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.summary.harvestCount))}</strong></div>
                <div>偷菜次数：<strong style="color:#111827;">${escapeHtml(formatNumberSafe(account.summary.stealCount))}</strong></div>
            </div>
            ${account.lastError ? `<p style="margin:12px 0 0;font-size:12px;color:#9f1239;">最近错误：${escapeHtml(account.lastError)}</p>` : ''}
        </div>
    `).join('');

    return `
        <div style="margin:0;padding:24px;background:#f5f7fb;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;color:#111827;">
            <div style="max-width:760px;margin:0 auto;">
                <div style="border-radius:18px;overflow:hidden;background:#ffffff;box-shadow:0 18px 36px rgba(15,23,42,0.08);">
                    <div style="padding:22px 24px;background:linear-gradient(135deg,#3268ff,#2ab0ff);color:#ffffff;">
                        <h1 style="margin:0;font-size:24px;">QQ农场${label}</h1>
                        <p style="margin:8px 0 0;font-size:13px;opacity:0.9;">时间：${escapeHtml(formatNotifyTime(now))}</p>
                    </div>
                    <div style="padding:20px 24px;">
                        <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;">
                            <div style="padding:14px;border-radius:14px;background:#eef4ff;">
                                <div style="font-size:12px;color:#5b6b83;">出售收益</div>
                                <div style="margin-top:6px;font-size:24px;font-weight:800;color:#132238;">${escapeHtml(formatNumberSafe(payload.totals.saleGold))}</div>
                            </div>
                            <div style="padding:14px;border-radius:14px;background:#eefcf6;">
                                <div style="font-size:12px;color:#5b6b83;">偷菜估值</div>
                                <div style="margin-top:6px;font-size:24px;font-weight:800;color:#132238;">${escapeHtml(formatNumberSafe(payload.totals.stealGold))}</div>
                            </div>
                            <div style="padding:14px;border-radius:14px;background:#fff5ee;">
                                <div style="font-size:12px;color:#5b6b83;">收获次数</div>
                                <div style="margin-top:6px;font-size:24px;font-weight:800;color:#132238;">${escapeHtml(formatNumberSafe(payload.totals.harvestCount))}</div>
                            </div>
                        </div>
                        ${accountCards}
                    </div>
                </div>
            </div>
        </div>
    `.trim();
}

function formatNumberSafe(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return '0';
    return n.toLocaleString('zh-CN');
}

function escapeHtml(text) {
    return String(text ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

async function sendDisconnectNotification(userState, reason, options = {}) {
    const config = readUserConfig(userState);
    const readiness = getDisconnectNotifyReadiness(config);
    const channels = readiness.channels;
    const toggles = readiness.toggles;
    if (readiness.readyChannelCount <= 0) {
        return {
            ok: false,
            skipped: true,
            reason: readiness.reasons[0] === '掉线提醒未启用'
                ? '掉线提醒未启用'
                : (readiness.reasons.join('；') || '没有启用的通知渠道'),
        };
    }

    const now = Date.now();
    const normalizedReason = String(reason || '未知原因').trim() || '未知原因';
    const lastState = disconnectNotifyState.get(userState.username);
    const sameReason = lastState && lastState.reason === normalizedReason;
    if (!options.skipDedupe && lastState && sameReason && (now - lastState.atMs) < 10 * 60 * 1000) {
        return { ok: false, skipped: true, reason: '10分钟内相同原因已提醒过' };
    }

    const title = `⚠️ QQ农场账号掉线提醒 - ${userState.username}`;
    const markdown = [
        `## 账号掉线提醒`,
        '',
        `- 账号：${userState.username}`,
        `- 时间：${formatNotifyTime(now)}`,
        `- 原因：${normalizedReason}`,
        `- 当前状态：${userState.runtime.running ? '运行中' : '已停止'}`,
    ].join('\n');
    const html = `
        <div style="margin:0;padding:24px;background:#f5f7fb;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;">
            <div style="max-width:620px;margin:0 auto;border-radius:18px;overflow:hidden;background:#ffffff;box-shadow:0 18px 36px rgba(15,23,42,0.08);">
                <div style="padding:22px 24px;background:linear-gradient(135deg,#d43b55,#ff7649);color:#ffffff;">
                    <h1 style="margin:0;font-size:22px;">账号掉线提醒</h1>
                </div>
                <div style="padding:20px 24px;color:#1f2937;">
                    <p style="margin:0 0 12px;">检测到 bot 从运行中异常退出。</p>
                    <table style="width:100%;border-collapse:collapse;font-size:14px;">
                        <tr><td style="padding:8px 0;color:#6b7280;">账号</td><td style="padding:8px 0;font-weight:700;">${escapeHtml(userState.username)}</td></tr>
                        <tr><td style="padding:8px 0;color:#6b7280;">时间</td><td style="padding:8px 0;font-weight:700;">${escapeHtml(formatNotifyTime(now))}</td></tr>
                        <tr><td style="padding:8px 0;color:#6b7280;">原因</td><td style="padding:8px 0;font-weight:700;color:#9f1239;">${escapeHtml(normalizedReason)}</td></tr>
                    </table>
                </div>
            </div>
        </div>
    `.trim();

    const result = await pushNotification({
        title,
        markdown,
        html,
        channels: {
            email: toggles.emailEnabled,
            serverChan: toggles.serverChanEnabled,
        },
    }, channels);

    if (!options.skipDedupe && !result.skipped) {
        disconnectNotifyState.set(userState.username, { atMs: now, reason: normalizedReason });
    }
    if (!options.quiet) {
        addUserLogLine(userState, `[通知] 掉线提醒结果: ${result.skipped ? result.reason : JSON.stringify(result.results)}`, true);
    }
    return result;
}

async function generateAndSendReport(userState, type = 'hourly', { force = false } = {}) {
    if (!userState) return { ok: false, skipped: true, reason: '账号不存在' };
    const normalizedType = type === 'daily' ? 'daily' : 'hourly';
    const config = readUserConfig(userState);
    const toggles = config.reportNotify || normalizeReportNotify();
    if (!force) {
        if (normalizedType === 'hourly' && !toggles.hourlyEnabled) return { ok: false, skipped: true, reason: '每小时汇报未启用' };
        if (normalizedType === 'daily' && !toggles.dailyEnabled) return { ok: false, skipped: true, reason: '每日汇报未启用' };
    }
    if (!toggles.emailEnabled && !toggles.serverChanEnabled) {
        return { ok: false, skipped: true, reason: '定时汇报未启用任何渠道' };
    }
    const payload = collectReportPayload(normalizedType, [userState]);
    const now = new Date();
    const title = `📊 QQ农场${normalizedType === 'daily' ? '每日' : '每小时'}汇报 - ${userState.username} - ${formatNotifyTime(now)}`;
    const markdown = buildReportMarkdown(normalizedType, payload, now);
    const html = buildReportHtml(normalizedType, payload, now);
    return pushNotification({
        title,
        markdown,
        html,
        channels: {
            email: toggles.emailEnabled,
            serverChan: toggles.serverChanEnabled,
        },
    }, config.notificationChannels || normalizeNotificationChannels());
}

function msUntilNextHour() {
    const now = new Date();
    const next = new Date(now);
    next.setMinutes(60, 0, 0);
    return Math.max(1000, next.getTime() - now.getTime());
}

function msUntilNextDailyHour(targetHour) {
    const hour = toInt(targetHour, 8, 0, 23);
    const now = new Date();
    const next = new Date(now);
    next.setHours(hour, 0, 0, 0);
    if (next.getTime() <= now.getTime()) {
        next.setDate(next.getDate() + 1);
    }
    return Math.max(1000, next.getTime() - now.getTime());
}

function clearReportSchedulers(userState = null) {
    if (userState && userState.username) {
        const hourlyTimer = hourlyReportTimers.get(userState.username);
        if (hourlyTimer) {
            clearTimeout(hourlyTimer);
            hourlyReportTimers.delete(userState.username);
        }
        const dailyTimer = dailyReportTimers.get(userState.username);
        if (dailyTimer) {
            clearTimeout(dailyTimer);
            dailyReportTimers.delete(userState.username);
        }
        return;
    }
    for (const timer of hourlyReportTimers.values()) clearTimeout(timer);
    for (const timer of dailyReportTimers.values()) clearTimeout(timer);
    hourlyReportTimers.clear();
    dailyReportTimers.clear();
}

function scheduleHourlyReport(userState) {
    if (!userState || !userState.username) return;
    const config = readUserConfig(userState);
    if (!(config.reportNotify && config.reportNotify.hourlyEnabled)) return;
    const delayMs = msUntilNextHour();
    const timer = setTimeout(async () => {
        try {
            await generateAndSendReport(userState, 'hourly');
        } catch (err) {
            console.error(`[通知] 每小时汇报发送失败(${userState.username}):`, err.message);
        } finally {
            scheduleHourlyReport(userState);
        }
    }, delayMs);
    hourlyReportTimers.set(userState.username, timer);
}

function scheduleDailyReport(userState) {
    if (!userState || !userState.username) return;
    const config = readUserConfig(userState);
    if (!(config.reportNotify && config.reportNotify.dailyEnabled)) return;
    const delayMs = msUntilNextDailyHour(config.reportNotify.dailyHour);
    const timer = setTimeout(async () => {
        try {
            await generateAndSendReport(userState, 'daily');
        } catch (err) {
            console.error(`[通知] 每日汇报发送失败(${userState.username}):`, err.message);
        } finally {
            scheduleDailyReport(userState);
        }
    }, delayMs);
    dailyReportTimers.set(userState.username, timer);
}

function refreshNotificationSchedulers(userState = null) {
    if (userState && userState.username) {
        clearReportSchedulers(userState);
        scheduleHourlyReport(userState);
        scheduleDailyReport(userState);
        return;
    }
    clearReportSchedulers();
    for (const knownUserState of getAllKnownUserStates()) {
        scheduleHourlyReport(knownUserState);
        scheduleDailyReport(knownUserState);
    }
}

function shouldSendDisconnectNotification(userState) {
    if (!userState || !userState.runtime) return false;
    if (userState.runtime.lastAction === 'stop') return false;
    if (Number(userState.metrics.level || 0) > 0) return true;
    return getUserRuntimeUptimeSec(userState) >= 60;
}

function maybeNotifyUnexpectedDisconnect(userState, reason) {
    if (!shouldSendDisconnectNotification(userState)) return;
    void sendDisconnectNotification(userState, reason || userState.runtime.lastError || 'bot 进程异常退出')
        .catch((err) => console.error(`[通知] 掉线提醒发送失败(${userState.username}):`, err.message));
}

function normalizeFriendInsightsSort(value) {
    const key = String(value || '').trim().toLowerCase();
    if (['value', 'steal', 'fail', 'recent', 'success', 'name', 'level'].includes(key)) return key;
    return 'value';
}

function normalizeFriendInsightsOrder(value) {
    return String(value || '').trim().toLowerCase() === 'asc' ? 'asc' : 'desc';
}

function ensureFriendInsight(map, friendName) {
    const name = String(friendName || '').trim();
    if (!name) return null;
    if (!map.has(name)) {
        map.set(name, {
            name,
            source: 'log',
            gid: null,
            level: null,
            gold: null,
            stealCount: 0,
            careCount: 0,
            prankCount: 0,
            failCount: 0,
            estimatedValue: 0,
            valuedStealCount: 0,
            unknownValueStealCount: 0,
            previewStealNum: null,
            previewDryNum: null,
            previewWeedNum: null,
            previewInsectNum: null,
            cropCounts: new Map(),
            lastSeenAt: null,
            lastStealAt: null,
        });
    }
    return map.get(name);
}

function registerFriendInsightAliases(aliasMap, friend, ...names) {
    if (!aliasMap || !friend) return;
    for (const raw of names) {
        const key = normalizeFriendStealNameKey(raw);
        if (!key) continue;
        if (!aliasMap.has(key)) {
            aliasMap.set(key, friend);
        }
    }
}

function findFriendInsightByGid(map, gid) {
    const id = Number(gid);
    if (!Number.isInteger(id) || id <= 0) return null;
    for (const item of map.values()) {
        if (Number(item && item.gid) === id) return item;
    }
    return null;
}

function findFriendInsightByName(map, aliasMap, friendName) {
    const name = String(friendName || '').trim();
    if (!name) return null;
    if (map.has(name)) return map.get(name);
    const key = normalizeFriendStealNameKey(name);
    if (!key) return null;
    return aliasMap.get(key) || null;
}

function normalizeFriendStealNameKey(name) {
    const text = String(name || '').trim().toLowerCase();
    if (!text) return '';
    return text.replace(/\s+/g, ' ');
}

function getFriendStealConfigKey(gid, name) {
    const id = Number(gid);
    if (Number.isInteger(id) && id > 0) return String(id);
    const nameKey = normalizeFriendStealNameKey(name);
    return nameKey ? `name:${nameKey}` : '';
}

function resolveFriendStealEnabled(configMap, gid, name) {
    const key = getFriendStealConfigKey(gid, name);
    if (!key) return { key: '', enabled: true };
    return {
        key,
        enabled: configMap[key] !== false,
    };
}

function buildFriendInsightsPayload(userState, query = {}) {
    const limit = toInt(query.limit || MAX_LOG_LINES, MAX_LOG_LINES, 100, MAX_LOG_LINES);
    const sort = normalizeFriendInsightsSort(query.sort);
    const order = normalizeFriendInsightsOrder(query.order);
    const config = readUserConfig(userState);
    const friendStealEnabledMap = normalizeFriendStealEnabledMap(config.friendStealEnabled);
    const snapshot = readUserFriendsSnapshot(userState);
    const statsSnapshot = readUserFriendStats(userState);
    const hasAccumulatedStats = Boolean(statsSnapshot && statsSnapshot.hasStats && Array.isArray(statsSnapshot.friends) && statsSnapshot.friends.length > 0);
    const logs = hasAccumulatedStats
        ? []
        : (Array.isArray(userState && userState.logs) ? userState.logs.slice(-limit) : []);

    const saleUnitMap = hasAccumulatedStats ? new Map() : buildSaleUnitValueMap(logs);
    const seedUnitMap = readSeedUnitValueMap();
    const friendMap = new Map();
    const friendAliasMap = new Map();
    const snapshotRows = Array.isArray(snapshot.friends) ? snapshot.friends : [];
    const snapshotGidSet = new Set();

    for (const row of snapshotRows) {
        if (!row || typeof row !== 'object') continue;
        const name = String(row.name || row.remark || row.nick || '').trim();
        const friend = ensureFriendInsight(friendMap, name);
        if (!friend) continue;
        friend.source = 'api';

        const gid = Number(row.gid);
        if (Number.isInteger(gid) && gid > 0) {
            friend.gid = gid;
            snapshotGidSet.add(gid);
        }

        const level = Number(row.level);
        if (Number.isFinite(level) && level >= 0) {
            friend.level = Math.floor(level);
        }

        const gold = Number(row.gold);
        if (Number.isFinite(gold) && gold >= 0) {
            friend.gold = Math.floor(gold);
        }

        const preview = row.preview && typeof row.preview === 'object' ? row.preview : {};
        const previewStealNum = Number(preview.stealPlantNum);
        const previewDryNum = Number(preview.dryNum);
        const previewWeedNum = Number(preview.weedNum);
        const previewInsectNum = Number(preview.insectNum);
        friend.previewStealNum = Number.isFinite(previewStealNum) ? Math.max(0, Math.floor(previewStealNum)) : null;
        friend.previewDryNum = Number.isFinite(previewDryNum) ? Math.max(0, Math.floor(previewDryNum)) : null;
        friend.previewWeedNum = Number.isFinite(previewWeedNum) ? Math.max(0, Math.floor(previewWeedNum)) : null;
        friend.previewInsectNum = Number.isFinite(previewInsectNum) ? Math.max(0, Math.floor(previewInsectNum)) : null;
        friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, snapshot.updatedAt);
        registerFriendInsightAliases(friendAliasMap, friend, row.name, row.remark, row.nick, name);
    }

    const statsRows = Array.isArray(statsSnapshot.friends) ? statsSnapshot.friends : [];
    for (const row of statsRows) {
        if (!row || typeof row !== 'object') continue;
        const gid = Number(row.gid);
        const rowName = String(row.name || '').trim();
        const nick = String(row.nick || '').trim();
        const remark = String(row.remark || '').trim();

        if (snapshot.hasSnapshot && Number.isInteger(gid) && gid > 0 && !snapshotGidSet.has(gid)) {
            continue;
        }

        let friend = findFriendInsightByGid(friendMap, gid);
        if (!friend) {
            friend = findFriendInsightByName(friendMap, friendAliasMap, rowName || remark || nick);
        }
        if (!friend) {
            if (snapshot.hasSnapshot) continue;
            friend = ensureFriendInsight(friendMap, rowName || remark || nick);
            if (!friend) continue;
        }

        registerFriendInsightAliases(friendAliasMap, friend, rowName, remark, nick);

        friend.source = snapshot.hasSnapshot ? 'api+stats' : 'stats';
        if (Number.isInteger(gid) && gid > 0) friend.gid = gid;
        if (!snapshot.hasSnapshot && rowName) friend.name = rowName;
        if (!snapshot.hasSnapshot && !rowName && (remark || nick)) {
            friend.name = remark || nick;
        }

        const level = Number(row.level);
        if (Number.isFinite(level) && level >= 0) {
            friend.level = Math.floor(level);
        }

        const stealCount = Number(row.stealCount || 0);
        const careCount = Number(row.careCount || 0);
        const prankCount = Number(row.prankCount || 0);
        const failCount = Number(row.failCount || 0);
        friend.stealCount = Number.isFinite(stealCount) ? Math.max(0, Math.floor(stealCount)) : friend.stealCount;
        friend.careCount = Number.isFinite(careCount) ? Math.max(0, Math.floor(careCount)) : friend.careCount;
        friend.prankCount = Number.isFinite(prankCount) ? Math.max(0, Math.floor(prankCount)) : friend.prankCount;
        friend.failCount = Number.isFinite(failCount) ? Math.max(0, Math.floor(failCount)) : friend.failCount;

        const previewStealNum = Number(row.previewStealNum);
        const previewDryNum = Number(row.previewDryNum);
        const previewWeedNum = Number(row.previewWeedNum);
        const previewInsectNum = Number(row.previewInsectNum);
        friend.previewStealNum = Number.isFinite(previewStealNum) ? Math.max(0, Math.floor(previewStealNum)) : friend.previewStealNum;
        friend.previewDryNum = Number.isFinite(previewDryNum) ? Math.max(0, Math.floor(previewDryNum)) : friend.previewDryNum;
        friend.previewWeedNum = Number.isFinite(previewWeedNum) ? Math.max(0, Math.floor(previewWeedNum)) : friend.previewWeedNum;
        friend.previewInsectNum = Number.isFinite(previewInsectNum) ? Math.max(0, Math.floor(previewInsectNum)) : friend.previewInsectNum;

        const lastVisitAt = String(row.lastVisitAt || '');
        const lastStealAt = String(row.lastStealAt || '');
        const lastFailAt = String(row.lastFailAt || '');
        friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, lastVisitAt || statsSnapshot.updatedAt);
        friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, lastFailAt || null);
        friend.lastStealAt = updateLatestTime(friend.lastStealAt, lastStealAt || null);

        const stealsByPlant = row.stealPlants && typeof row.stealPlants === 'object' ? row.stealPlants : {};
        for (const [cropNameRaw, countRaw] of Object.entries(stealsByPlant)) {
            const cropName = normalizeCropName(cropNameRaw);
            if (!cropName) continue;
            const count = Number(countRaw);
            if (!Number.isFinite(count) || count <= 0) continue;
            friend.cropCounts.set(cropName, (friend.cropCounts.get(cropName) || 0) + count);
        }
    }

    for (const entry of logs) {
        const line = String(entry && entry.text ? entry.text : '');
        if (!line || !line.includes('[好友]')) continue;

        const lineTime = entry && entry.time ? entry.time : null;

        const newFriendMatch = line.match(/\[好友\]\s+新好友[:：]\s*(.+)$/);
        if (newFriendMatch) {
            let friend = findFriendInsightByName(friendMap, friendAliasMap, newFriendMatch[1]);
            if (!friend && !snapshot.hasSnapshot) {
                friend = ensureFriendInsight(friendMap, newFriendMatch[1]);
                registerFriendInsightAliases(friendAliasMap, friend, newFriendMatch[1]);
            }
            if (friend) friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, lineTime);
            continue;
        }

        const failMatch = line.match(/\[好友\].*进入(?:好友)?\s+(.+?)\s+农场失败[:：]/);
        if (failMatch) {
            let friend = findFriendInsightByName(friendMap, friendAliasMap, failMatch[1]);
            if (!friend && !snapshot.hasSnapshot) {
                friend = ensureFriendInsight(friendMap, failMatch[1]);
                registerFriendInsightAliases(friendAliasMap, friend, failMatch[1]);
            }
            if (friend) {
                friend.failCount += 1;
                friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, lineTime);
            }
            continue;
        }

        const detailMatch = line.match(/\[好友\]\s+(.+?)[:：]\s*(.+)$/);
        if (!detailMatch) continue;

        let friend = findFriendInsightByName(friendMap, friendAliasMap, detailMatch[1]);
        if (!friend && !snapshot.hasSnapshot) {
            friend = ensureFriendInsight(friendMap, detailMatch[1]);
            registerFriendInsightAliases(friendAliasMap, friend, detailMatch[1]);
        }
        if (!friend) continue;
        const actionText = String(detailMatch[2] || '').trim();
        friend.lastSeenAt = updateLatestTime(friend.lastSeenAt, lineTime);

        const levelMatch =
            actionText.match(/\bLv\.?\s*(\d{1,4})\b/i)
            || actionText.match(/等级[:：]?\s*(\d{1,4})/);
        if (levelMatch) {
            const level = Number(levelMatch[1]);
            if (Number.isInteger(level) && level >= 0) friend.level = level;
        }

        const goldMatch = actionText.match(/金币[:：]?\s*(\d{1,12})/);
        if (goldMatch) {
            const gold = Number(goldMatch[1]);
            if (Number.isFinite(gold) && gold >= 0) friend.gold = gold;
        }

        const stealCount = extractActionCount(actionText, '偷菜');
        const careCountSummary = extractActionCount(actionText, '照顾');
        const prankCountSummary = extractActionCount(actionText, '捣乱');
        const careCountDerived = extractActionCount(actionText, '浇水') + extractActionCount(actionText, '除草') + extractActionCount(actionText, '除虫');
        const prankCountDerived = extractActionCount(actionText, '放草') + extractActionCount(actionText, '放虫');
        const careCount = careCountSummary > 0 ? careCountSummary : careCountDerived;
        const prankCount = prankCountSummary > 0 ? prankCountSummary : prankCountDerived;

        friend.stealCount += stealCount;
        friend.careCount += careCount;
        friend.prankCount += prankCount;

        if (stealCount > 0) {
            friend.lastStealAt = updateLatestTime(friend.lastStealAt, lineTime);
            const stealPlantsMatch = actionText.match(/偷菜\d+\(([^)]+)\)/);
            const plantNames = parseStealPlantNames(stealPlantsMatch ? stealPlantsMatch[1] : '');

            if (plantNames.length > 0) {
                const share = stealCount / plantNames.length;
                const unitValues = [];
                for (const plantName of plantNames) {
                    const key = normalizeCropName(plantName);
                    if (!key) continue;
                    friend.cropCounts.set(key, (friend.cropCounts.get(key) || 0) + share);
                    const v = Number(saleUnitMap.get(key) || seedUnitMap.get(key) || 0);
                    if (Number.isFinite(v) && v > 0) unitValues.push(v);
                }
                if (unitValues.length > 0) {
                    const avgUnit = unitValues.reduce((acc, n) => acc + n, 0) / unitValues.length;
                    friend.estimatedValue += stealCount * avgUnit;
                    friend.valuedStealCount += stealCount;
                } else {
                    friend.unknownValueStealCount += stealCount;
                }
            } else {
                friend.unknownValueStealCount += stealCount;
            }
        }
    }

    const list = Array.from(friendMap.values()).map((item) => {
        const cropEntries = Array.from(item.cropCounts.entries());
        const crops = cropEntries
            .sort((a, b) => b[1] - a[1])
            .slice(0, 4)
            .map(([name, count]) => ({
                name,
                count: roundTo(count, 1),
            }));

        let estimatedValueFromCrops = 0;
        let valuedStealCount = 0;
        let unknownValueStealCount = 0;
        for (const [name, countRaw] of cropEntries) {
            const count = Number(countRaw);
            if (!Number.isFinite(count) || count <= 0) continue;
            const unit = Number(saleUnitMap.get(name) || seedUnitMap.get(name) || 0);
            if (Number.isFinite(unit) && unit > 0) {
                estimatedValueFromCrops += count * unit;
                valuedStealCount += count;
            } else {
                unknownValueStealCount += count;
            }
        }

        const estimatedValue = estimatedValueFromCrops > 0 ? estimatedValueFromCrops : Number(item.estimatedValue || 0);
        const value = roundTo(estimatedValue, 2);
        const valuedCount = valuedStealCount > 0 ? valuedStealCount : Number(item.valuedStealCount || 0);
        const unknownCount = unknownValueStealCount > 0 ? unknownValueStealCount : Number(item.unknownValueStealCount || 0);
        const valuePerSteal = valuedCount > 0
            ? roundTo(estimatedValue / valuedCount, 2)
            : null;
        const stealControl = resolveFriendStealEnabled(friendStealEnabledMap, item.gid, item.name);

        return {
            name: item.name,
            source: item.source,
            gid: item.gid,
            level: item.level,
            gold: item.gold,
            stealKey: stealControl.key,
            stealEnabled: stealControl.enabled,
            stealCount: item.stealCount,
            careCount: item.careCount,
            prankCount: item.prankCount,
            failCount: item.failCount,
            estimatedValue: value,
            valuePerSteal,
            valuedStealCount: roundTo(valuedCount, 1),
            unknownValueStealCount: roundTo(unknownCount, 1),
            previewStealNum: item.previewStealNum,
            previewDryNum: item.previewDryNum,
            previewWeedNum: item.previewWeedNum,
            previewInsectNum: item.previewInsectNum,
            crops,
            lastSeenAt: item.lastSeenAt,
            lastStealAt: item.lastStealAt,
        };
    });

    const compareBy = (a, b) => {
        if (sort === 'name') {
            return a.name.localeCompare(b.name, 'zh-Hans-CN', { sensitivity: 'base' });
        }
        if (sort === 'level') return Number(a.level || 0) - Number(b.level || 0);
        if (sort === 'steal') return a.stealCount - b.stealCount;
        if (sort === 'fail') return a.failCount - b.failCount;
        if (sort === 'recent') return toTimestamp(a.lastSeenAt) - toTimestamp(b.lastSeenAt);
        if (sort === 'success') return toTimestamp(a.lastStealAt) - toTimestamp(b.lastStealAt);
        return a.estimatedValue - b.estimatedValue;
    };

    list.sort((a, b) => {
        const diff = compareBy(a, b);
        if (diff !== 0) return order === 'asc' ? diff : -diff;
        const tie = b.stealCount - a.stealCount;
        if (tie !== 0) return tie;
        return a.name.localeCompare(b.name, 'zh-Hans-CN', { sensitivity: 'base' });
    });

    const totalStealCount = list.reduce((sum, item) => sum + item.stealCount, 0);
    const totalCareCount = list.reduce((sum, item) => sum + item.careCount, 0);
    const totalPrankCount = list.reduce((sum, item) => sum + item.prankCount, 0);
    const totalFailCount = list.reduce((sum, item) => sum + item.failCount, 0);
    const totalEstimatedValue = roundTo(list.reduce((sum, item) => sum + item.estimatedValue, 0), 2);
    const lowValueFriendCount = list.filter((item) => item.stealCount > 0 && item.estimatedValue <= 0).length;
    const noStealFriendCount = list.filter((item) => item.stealCount <= 0).length;
    const apiFriendCount = snapshot.hasSnapshot ? snapshotRows.length : 0;
    const statsFriendCount = hasAccumulatedStats ? statsRows.length : 0;

    return {
        ok: true,
        generatedAt: new Date().toISOString(),
        sort,
        order,
        source: {
            logWindow: logs.length,
            hasApiSnapshot: Boolean(snapshot.hasSnapshot),
            apiFriendCount,
            apiSnapshotUpdatedAt: snapshot.updatedAt || null,
            hasAccumulatedStats,
            statsFriendCount,
            statsUpdatedAt: statsSnapshot.updatedAt || null,
            note: hasAccumulatedStats
                ? `行为统计来自累计快照（${statsFriendCount} 人）；当前好友总名单仅展示官方接口快照内好友（${apiFriendCount} 人）`
                : (snapshot.hasSnapshot
                    ? `好友总名单来自官方接口快照（${apiFriendCount} 人），行为与价值基于最近 ${logs.length} 条日志估算`
                    : `暂未读取到官方好友快照，当前仅基于最近 ${logs.length} 条日志估算`),
        },
        overview: {
            friendCount: list.length,
            totalStealCount,
            totalCareCount,
            totalPrankCount,
            totalFailCount,
            totalEstimatedValue,
            lowValueFriendCount,
            noStealFriendCount,
        },
        friends: list,
    };
}

// ============ Express Setup ============

app.use(express.json({ limit: '256kb' }));
app.use(express.static(PUBLIC_ROOT));

// ---- Auth Routes ----

app.post('/api/auth/register', (req, res) => {
    const adminSettings = readAdminSettings();
    if (!adminSettings.registrationEnabled) {
        return res.status(403).json({ error: '当前不允许注册新账号' });
    }

    const username = String(req.body && req.body.username || '').trim().toLowerCase();
    const password = String(req.body && req.body.password || '');

    if (!username || !/^[a-z0-9_-]{2,32}$/.test(username)) {
        return res.status(400).json({ error: '用户名只允许字母/数字/下划线/短横线，长度 2-32' });
    }
    if (!password || password.length < 4 || password.length > 128) {
        return res.status(400).json({ error: '密码长度需在 4-128 之间' });
    }

    const users = loadUsers();
    if (users.find((u) => u.username === username)) {
        return res.status(409).json({ error: '用户名已被注册' });
    }

    const passwordHash = hashPassword(password);
    users.push({ username, passwordHash, createdAt: new Date().toISOString() });
    saveUsers(users);

    fs.mkdirSync(getUserDataDir(username), { recursive: true });

    const token = generateToken();
    sessions.set(token, { username, createdAt: Date.now() });
    setCookieHeader(res, token);
    res.status(201).json({ message: '注册成功', username });
});

app.post('/api/auth/login', (req, res) => {
    const username = String(req.body && req.body.username || '').trim().toLowerCase();
    const password = String(req.body && req.body.password || '');

    const users = loadUsers();
    const user = users.find((u) => u.username === username);
    if (!user || !verifyPassword(password, user.passwordHash)) {
        return res.status(401).json({ error: '用户名或密码错误' });
    }

    const token = generateToken();
    sessions.set(token, { username, createdAt: Date.now() });
    setCookieHeader(res, token);
    res.json({ message: '登录成功', username });
});

app.post('/api/auth/logout', (req, res) => {
    const token = parseCookies(req).session;
    if (token) sessions.delete(token);
    clearCookieHeader(res);
    res.json({ message: '已退出登录' });
});

app.get('/api/auth/registration-status', (req, res) => {
    const { registrationEnabled } = readAdminSettings();
    res.json({ registrationEnabled: !!registrationEnabled });
});

app.get('/api/auth/me', (req, res) => {
    const token = parseCookies(req).session;
    const session = token && sessions.get(token);
    if (!session) return res.status(401).json({ error: '未登录' });
    const users = loadUsers();
    const user = users.find((u) => u.username === session.username);
    res.json({ username: session.username, isAdmin: !!(user && user.isAdmin) });
});

app.get('/api/admin/status', requireAuth, (req, res) => {
    // Count unique logged-in usernames
    const loggedInSet = new Set();
    for (const session of sessions.values()) {
        loggedInSet.add(session.username);
    }
    // Count users with running bots
    let running = 0;
    for (const [, userState] of userStates) {
        if (userState.runtime.running) running++;
    }
    res.json({ loggedIn: loggedInSet.size, running });
});

// ---- Admin API Routes ----

app.get('/api/admin/users', requireAdmin, (req, res) => {
    const users = loadUsers();
    const loggedInSet = new Set();
    for (const session of sessions.values()) {
        loggedInSet.add(session.username);
    }
    const result = users.map((u) => {
        const userState = userStates.get(u.username);
        return {
            username: u.username,
            createdAt: u.createdAt,
            isAdmin: !!u.isAdmin,
            loggedIn: loggedInSet.has(u.username),
            botRunning: !!(userState && userState.runtime.running),
        };
    });
    res.json({ users: result });
});

app.get('/api/admin/settings', requireAdmin, (req, res) => {
    res.json(readAdminSettings());
});

app.post('/api/admin/settings', requireAdmin, (req, res) => {
    const current = readAdminSettings();
    const body = (req.body && typeof req.body === 'object') ? req.body : {};
    const next = { ...current };
    if (typeof body.registrationEnabled === 'boolean') {
        next.registrationEnabled = body.registrationEnabled;
    }
    saveAdminSettings(next);
    res.json({ ok: true, ...next });
});

app.delete('/api/admin/users/:username', requireAdmin, async (req, res) => {
    const target = String(req.params.username || '').trim().toLowerCase();
    if (!target) return res.status(400).json({ error: '用户名无效' });
    if (target === req.username) return res.status(400).json({ error: '不能删除自己的账号' });

    const users = loadUsers();
    const idx = users.findIndex((u) => u.username === target);
    if (idx < 0) return res.status(404).json({ error: '用户不存在' });

    // Stop the bot if running
    const userState = userStates.get(target);
    if (userState && userState.runtime.running) {
        try { await stopUserBot(userState); } catch (e) { /* ignore */ }
    }

    // Invalidate all sessions for that user
    for (const [token, session] of sessions.entries()) {
        if (session.username === target) sessions.delete(token);
    }

    // Remove from users list
    users.splice(idx, 1);
    saveUsers(users);

    // Remove user state from memory
    userStates.delete(target);

    // Remove user data directory
    const userDir = path.join(DATA_ROOT, 'users', target);
    if (fs.existsSync(userDir)) {
        fs.rmSync(userDir, { recursive: true, force: true });
    }

    res.json({ ok: true, message: `用户 ${target} 已删除` });
});

// ---- Protected API Routes ----

app.get('/api/health', requireAuth, (req, res) => {
    res.json({ ok: true, now: new Date().toISOString(), botRoot: BOT_ROOT, username: req.username });
});

app.get('/api/config', requireAuth, (req, res) => {
    res.json(buildClientConfig(readUserConfig(req.userState)));
});

app.post('/api/config', requireAuth, (req, res) => {
    const current = readUserConfig(req.userState);
    const body = (req.body && typeof req.body === 'object') ? req.body : {};
    const nextInput = { ...current, ...body };

    if (
        Object.prototype.hasOwnProperty.call(body, 'code')
        || Object.prototype.hasOwnProperty.call(body, 'clientVersion')
    ) {
        const nextCode = Object.prototype.hasOwnProperty.call(body, 'code')
            ? String(body.code || '').trim()
            : current.code;
        const nextClientVersion = Object.prototype.hasOwnProperty.call(body, 'clientVersion')
            ? normalizeClientVersion(body.clientVersion)
            : current.clientVersion;
        if (nextCode !== current.code || nextClientVersion !== current.clientVersion) {
            const hasCompleteLoginConfig = Boolean(nextCode && nextClientVersion);
            nextInput.codeStatus = hasCompleteLoginConfig ? 'ready' : 'empty';
            nextInput.codeStatusReason = '';
            nextInput.codeStatusMessage = '';
            nextInput.codeUpdatedAt = hasCompleteLoginConfig ? new Date().toISOString() : '';
            if (!hasCompleteLoginConfig) {
                nextInput.codeLastUsedAt = '';
                nextInput.codeLastErrorAt = '';
            }
        }
    }

    const next = writeUserConfig(req.userState, nextInput);
    res.json({ message: '配置已保存', config: buildClientConfig(next) });
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
});

app.get('/api/notification-settings', requireAuth, (req, res) => {
    const current = readUserConfig(req.userState);
    res.json({
        ok: true,
        settings: {
            notificationChannels: current.notificationChannels || normalizeNotificationChannels(),
            disconnectNotify: current.disconnectNotify || normalizeDisconnectNotify(),
            reportNotify: current.reportNotify || normalizeReportNotify(),
        },
    });
});

app.post('/api/notification-settings', requireAuth, (req, res) => {
    const current = readUserConfig(req.userState);
    const body = (req.body && typeof req.body === 'object') ? req.body : {};
    const next = writeUserConfig(req.userState, {
        ...current,
        ...body,
        notificationChannels: {
            ...current.notificationChannels,
            ...(body.notificationChannels || {}),
        },
        disconnectNotify: {
            ...current.disconnectNotify,
            ...(body.disconnectNotify || {}),
        },
        reportNotify: {
            ...current.reportNotify,
            ...(body.reportNotify || {}),
        },
    });
    refreshNotificationSchedulers(req.userState);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        message: '当前账号通知设置已保存',
        settings: {
            notificationChannels: next.notificationChannels || normalizeNotificationChannels(),
            disconnectNotify: next.disconnectNotify || normalizeDisconnectNotify(),
            reportNotify: next.reportNotify || normalizeReportNotify(),
        },
    });
});

app.post('/api/notification-settings/test-disconnect', requireAuth, async (req, res, next) => {
    try {
        const reason = String(req.body && req.body.reason || '手动触发测试提醒').trim() || '手动触发测试提醒';
        const result = await sendDisconnectNotification(req.userState, reason, { quiet: false, skipDedupe: true });
        res.json({ ok: true, result });
    } catch (err) {
        next(err);
    }
});

app.post('/api/notification-settings/test-report', requireAuth, async (req, res, next) => {
    try {
        const type = String(req.body && req.body.type || 'hourly').trim().toLowerCase() === 'daily' ? 'daily' : 'hourly';
        const result = await generateAndSendReport(req.userState, type, { force: true });
        res.json({ ok: true, type, result });
    } catch (err) {
        next(err);
    }
});

app.get('/api/runtime', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    res.json(buildUserRuntimePayload(req.userState));
});

app.get('/api/stats/overview', requireAuth, (req, res, next) => {
    try {
        res.json(buildOverviewStatsPayload(req.userState));
    } catch (err) {
        next(err);
    }
});

app.get('/api/lands', requireAuth, (req, res) => {
    res.json(readUserLandsSnapshot(req.userState));
});

app.post('/api/lands/upgrade-all-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行升级地块请求' });
    }

    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        landUpgradeSweepRequestId: requestId,
        landUpgradeSweepRequestedAtMs: now,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求尝试升级所有地块一次 requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        message: '已请求尝试升级所有地块一次，将在下一轮巡田执行',
    });
});

app.post('/api/mall/claim-daily-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行每日福利领取请求' });
    }

    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        mallDailyClaimRequestId: requestId,
        mallDailyClaimRequestedAtMs: now,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求领取每日福利一次 requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        message: '已请求领取每日福利一次，将在下一轮巡田执行',
    });
});

app.post('/api/mall/buy-fert10h-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行10小时化肥购买请求' });
    }

    const count = toInt(req.body && req.body.count, 1, 1, 999);
    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        mallBuy10hFertRequestId: requestId,
        mallBuy10hFertRequestedAtMs: now,
        mallBuy10hFertCount: count,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求购买10小时化肥 x${count} requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        count,
        message: `已请求购买10小时化肥 x${count}，将在下一轮巡田执行`,
    });
});

app.post('/api/items/use-all-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行仓库道具一键使用请求' });
    }

    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        bagUseAllRequestId: requestId,
        bagUseAllRequestedAtMs: now,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求一键使用仓库道具 requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        message: '已请求一键使用仓库道具，将在下一轮巡田执行',
    });
});

app.post('/api/items/refresh-bag-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法刷新仓库快照' });
    }

    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        bagSnapshotRequestId: requestId,
        bagSnapshotRequestedAtMs: now,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求刷新仓库快照 requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        requestedAtMs: now,
        message: '已请求刷新仓库快照，将在下一轮巡田执行',
    });
});

app.get('/api/items/bag-snapshot', requireAuth, (req, res) => {
    res.json(readUserBagSnapshot(req.userState));
});

app.post('/api/items/use-selected-once', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行仓库道具打开请求' });
    }

    const items = normalizeBagUseSelectedItems(req.body && req.body.items);
    if (!Array.isArray(items) || items.length <= 0) {
        return res.status(400).json({ error: '请至少选择一个道具' });
    }

    const current = readUserConfig(req.userState);
    const now = Date.now();
    const requestId = `${now}-${Math.random().toString(36).slice(2, 8)}`;
    const next = writeUserConfig(req.userState, {
        ...current,
        bagUseSelectedRequestId: requestId,
        bagUseSelectedRequestedAtMs: now,
        bagUseSelectedItems: items,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求打开勾选道具 ${items.length} 项 requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        requestedAtMs: now,
        selectedCount: items.length,
        message: `已请求打开勾选道具 ${items.length} 项，将在下一轮巡田执行`,
    });
});

app.get('/api/logs', requireAuth, (req, res) => {
    const limit = toInt(req.query.limit || 300, 300, 1, 2000);
    res.json({ logs: req.userState.logs.slice(-limit) });
});

app.get('/api/friends/insights', requireAuth, (req, res, next) => {
    try {
        res.json(buildFriendInsightsPayload(req.userState, req.query || {}));
    } catch (err) {
        next(err);
    }
});

app.post('/api/friends/delete', requireAuth, (req, res) => {
    refreshUserRuntime(req.userState, false);
    if (!req.userState.runtime.running) {
        return res.status(409).json({ error: 'bot 未运行，无法执行删好友请求' });
    }

    const gid = toInt(req.body && req.body.gid, 0, 1, 9999999999999);
    if (gid <= 0) {
        return res.status(400).json({ error: '缺少有效的好友 gid' });
    }

    const name = String(req.body && req.body.name || '').trim().slice(0, 80);
    const current = readUserConfig(req.userState);
    const existingQueue = normalizeFriendDeleteRequests(current.friendDeleteRequests);
    const existing = existingQueue.find((item) => item.gid === gid);
    const displayName = name || (existing && existing.name) || `GID:${gid}`;

    if (existing) {
        return res.json({
            ok: true,
            alreadyPending: true,
            requestId: existing.requestId,
            gid,
            message: `好友 ${displayName} 已在删除队列中，等待 bot 执行`,
        });
    }

    const now = Date.now();
    const requestId = `${now}-${gid}-${Math.random().toString(36).slice(2, 8)}`;
    const nextQueue = [...existingQueue, {
        requestId,
        gid,
        name,
        requestedAtMs: now,
    }].slice(-50);
    const next = writeUserConfig(req.userState, {
        ...current,
        friendDeleteRequests: nextQueue,
    });

    addUserLogLine(req.userState, `[WEBUI] 已请求删除好友 ${displayName} gid=${gid} requestId=${requestId}`, true);
    broadcastToUser(req.userState, 'config', buildClientConfig(next));
    res.json({
        ok: true,
        requestId,
        gid,
        message: `已请求删除好友 ${displayName}，将在下一轮好友处理执行`,
    });
});

app.get('/api/share', requireAuth, (req, res) => {
    res.json({ content: readUserShareContent(req.userState) });
});

app.post('/api/share', requireAuth, (req, res) => {
    const content = String(req.body && req.body.content ? req.body.content : '');
    if (content.length > 50000) {
        return res.status(400).json({ error: 'share 内容过长（最大 50000 字符）' });
    }
    writeUserShareContent(req.userState, content);
    addUserLogLine(req.userState, '[WEBUI] share.txt 已更新', true);
    res.json({ message: 'share.txt 已保存', content });
});

app.get('/api/plant-options', requireAuth, (req, res) => {
    res.json({ options: readPlantOptions() });
});

app.get('/api/farmcalc/recommendation', requireAuth, (req, res, next) => {
    try {
        const payload = buildFarmCalcRecommendation(req.query || {}, req.userState);
        res.json(payload);
    } catch (err) {
        next(err);
    }
});

app.get('/api/code-auto/status', requireAuth, (req, res) => {
    tickAutoCodeSession();
    res.json({
        ok: true,
        ...getAutoCodeStatusForUser(req.username, { consumeResult: true }),
    });
});

app.post('/api/code-auto/start', requireAuth, (req, res) => {
    tickAutoCodeSession();
    const current = autoCodeSession;
    if (current && current.owner !== req.username) {
        const snapshot = getAutoCodeStatusForUser(req.username, { consumeResult: true });
        return res.status(409).json({
            ok: false,
            error: '如果是已锁定，请稍等，有其他人正在使用，一分钟后再试试。',
            ...snapshot,
        });
    }

    if (!current) {
        const cleared = clearAutoCodeFile();
        const deleteWarning = cleared.ok ? '' : `删除旧文件失败: ${cleared.error}`;
        startAutoCodeSession(req.username, deleteWarning);
        addUserLogLine(req.userState, `[WEBUI] 已启动自动获取 code/版本号（3分钟） file=${AUTO_CODE_FILE_PATH}`, true);
    }

    tickAutoCodeSession();
    res.json({
        ok: true,
        message: '自动获取 code/版本号 已启动，正在监听...',
        ...getAutoCodeStatusForUser(req.username, { consumeResult: true }),
    });
});

app.post('/api/code-auto/stop', requireAuth, (req, res) => {
    const current = autoCodeSession;
    if (current && current.owner === req.username) {
        finishAutoCodeSession('manual-stop', {});
        addUserLogLine(req.userState, '[WEBUI] 已停止自动获取 code/版本号', true);
    }
    res.json({
        ok: true,
        ...getAutoCodeStatusForUser(req.username, { consumeResult: true }),
    });
});

app.post('/api/bot/start', requireAuth, async (req, res, next) => {
    try {
        const result = await startUserBot(req.userState);
        res.json(result);
    } catch (err) {
        next(err);
    }
});

app.post('/api/bot/stop', requireAuth, async (req, res, next) => {
    try {
        const result = await stopUserBot(req.userState);
        res.json(result);
    } catch (err) {
        next(err);
    }
});

app.get('/api/events', requireAuth, (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const userState = req.userState;
    userState.clients.add(res);

    refreshUserRuntime(userState, false);
    sseWrite(res, 'runtime', buildUserRuntimePayload(userState));
    sseWrite(res, 'metrics', userState.metrics);

    const keepAlive = setInterval(() => {
        sseWrite(res, 'ping', { now: Date.now() });
    }, 15000);

    // In newer Node versions, IncomingMessage 'close' can fire when the request
    // finishes reading (for GET/SSE that's almost immediate). Clean up on the
    // response/socket close instead so the SSE client keeps receiving events.
    const cleanup = () => {
        clearInterval(keepAlive);
        userState.clients.delete(res);
    };

    res.on('close', cleanup);
    res.on('error', cleanup);
    req.on('aborted', cleanup);
});

// Serve login.html for /login
app.get('/login', (req, res) => {
    res.sendFile(path.join(PUBLIC_ROOT, 'login.html'));
});

// Serve admin.html for /admin
app.get('/admin', (req, res) => {
    res.sendFile(path.join(PUBLIC_ROOT, 'admin.html'));
});

// Catch-all: serve index.html (frontend handles auth redirect)
app.get('*', (req, res) => {
    res.sendFile(path.join(PUBLIC_ROOT, 'index.html'));
});

app.use((err, req, res, next) => {
    const code = Number(err.statusCode || 500);
    const message = err.message || '服务器内部错误';
    res.status(code).json({ error: message });
});

// ============ Bootstrap ============

function bootstrap() {
    fs.mkdirSync(DATA_ROOT, { recursive: true });

    if (!fs.existsSync(BOT_ROOT)) {
        throw new Error(`未找到 qq-farm-bot 目录: ${BOT_ROOT}`);
    }
    if (!fs.existsSync(FARM_CALC_PATH)) {
        throw new Error(`Missing FarmCalc script: ${FARM_CALC_PATH}`);
    }

    // Load initial log tails for all existing users
    const existingUsers = loadUsers();
    for (const user of existingUsers) {
        const userState = getUserState(user.username);
        loadUserInitialLogTail(userState);
        refreshUserRuntime(userState, false);
    }

    refreshNotificationSchedulers();

    // Per-user log polling
    setInterval(() => {
        for (const [, userState] of userStates) {
            try { pollUserLogAppend(userState); } catch (e) {
                userState.runtime.lastError = `日志读取失败: ${e.message}`;
            }
        }
    }, 1200);

    // Per-user runtime polling
    setInterval(() => {
        for (const [, userState] of userStates) {
            refreshUserRuntime(userState);
        }
    }, 1000);
}

bootstrap();

process.on('SIGINT', () => {
    clearReportSchedulers();
    terminateDebugLabProcess();
    terminateDebugMitmProcess();
    process.exit(0);
});

process.on('SIGTERM', () => {
    clearReportSchedulers();
    terminateDebugLabProcess();
    terminateDebugMitmProcess();
    process.exit(0);
});

app.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`[WEBUI] http://localhost:${PORT}`);
    // eslint-disable-next-line no-console
    console.log(`[WEBUI] bot root: ${BOT_ROOT}`);
    // eslint-disable-next-line no-console
    console.log(`[WEBUI] multi-user mode enabled`);
});
