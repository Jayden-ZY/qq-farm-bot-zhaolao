/**
 * WebSocket 缃戠粶灞?- 杩炴帴/娑堟伅缂栬В鐮?登录/心跳
 */

const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const EventEmitter = require('events');
const { CONFIG } = require('./config');
const { types } = require('./proto');
const cryptoWasm = require('./crypto-wasm');
const { toLong, toNum, syncServerTime, log, logWarn, sanitizeLogText } = require('./utils');
const { updateStatusFromLogin, updateStatusGold, updateStatusLevel } = require('./status');
const { runLoginWarmup } = require('./sessionWarmup');

// ============ 浜嬩欢鍙戝皠鍣?(鐢ㄤ簬鎺ㄩ€侀€氱煡) ============
const networkEvents = new EventEmitter();

// ============ 鍐呴儴鐘舵€?============
let ws = null;
let clientSeq = 1;
let serverSeq = 0;
let heartbeatTimer = null;
let pendingCallbacks = new Map();
let reconnectTimer = null;
let reconnectAttempts = 0;
let reconnectScheduling = false;
let manualClosed = false;
let connectCode = '';
let connectOpenID = '';
let loginSuccessHandler = null;
let initialLoginDone = false;
let fatalStopRequested = false;
let reconnectStopKind = '';
let reconnectStopDetail = '';
let activeLoginBodySource = '';

const RECONNECT_BASE_DELAY = 2000;
const RECONNECT_MAX_DELAY = 30000;
const RECONNECT_MAX_RETRIES = 3;
const ASYNC_SEND_DEFAULT_TIMEOUT_MS = 15000;
const ASYNC_SEND_THROTTLE_BASE_GAP_MS = 24;
const ASYNC_SEND_THROTTLE_MAX_GAP_MS = 700;
const ASYNC_SEND_TIMEOUT_PENALTY_STEP_MS = 130;
const ASYNC_SEND_TIMEOUT_PENALTY_MAX_MS = 900;
const ASYNC_SEND_RECOVERY_STEP_MS = 20;
const ASYNC_SEND_SLOT_WAIT_MS = 16;
const ASYNC_SEND_PENDING_PENALTY_MS = [0, 40, 120, 240];
const ASYNC_TIMEOUT_STREAK_TRIGGER = 3;
const ASYNC_TIMEOUT_STREAK_RESET_MS = 30000;
const ASYNC_TIMEOUT_RECONNECT_COOLDOWN_MS = 8000;

let asyncSendPenaltyMs = 0;
let lastAsyncSendAt = 0;
let asyncSendQueue = Promise.resolve();
let asyncTimeoutStreak = 0;
let lastAsyncTimeoutAt = 0;
let lastAsyncReconnectAt = 0;

// ============ 鐢ㄦ埛鐘舵€?(登录鍚庤缃? ============
const userState = {
    gid: 0,
    name: '',
    openId: '',
    avatarUrl: '',
    level: 0,
    gold: 0,
    exp: 0,
};

function getUserState() { return userState; }

function resetSeqState() {
    clientSeq = 1;
    serverSeq = 0;
}

function clearHeartbeatTimer() {
    if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
    }
}

function clearPendingCallbacks(message = '连接已断开，已清理') {
    pendingCallbacks.forEach((cb) => {
        try { cb(new Error(message)); } catch (e) { }
    });
    pendingCallbacks.clear();
}

// ============ 娑堟伅缂栬В鐮?============
function setReconnectStopReason(kind = '', detail = '') {
    reconnectStopKind = String(kind || '').trim();
    reconnectStopDetail = String(detail || '').trim();
}

function clearReconnectStopReason() {
    reconnectStopKind = '';
    reconnectStopDetail = '';
}

function parseHexEscapes(text) {
    const out = [];
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (ch === '\\' && text[i + 1] === 'x' && /^[0-9a-fA-F]{2}$/.test(text.slice(i + 2, i + 4))) {
            out.push(parseInt(text.slice(i + 2, i + 4), 16));
            i += 3;
            continue;
        }
        if (/\s/.test(ch)) continue;
        out.push(ch.charCodeAt(0) & 0xff);
    }
    return Buffer.from(out);
}

function tryExtractLoginBodyFromGate(buf) {
    try {
        const msg = types.GateMessage.decode(buf);
        if (msg && msg.meta && /UserService$/.test(msg.meta.service_name || '') && msg.meta.method_name === 'Login' && msg.body) {
            return { body: Buffer.from(msg.body), encrypted: true };
        }
    } catch (e) { }
    return null;
}

function decodeExternalLoginPayload(rawBuf) {
    const direct = tryExtractLoginBodyFromGate(rawBuf);
    if (direct) return direct;

    const text = rawBuf.toString('utf8').trim().replace(/^\uFEFF/, '');
    if (!text) return null;

    const stripped = text.replace(/\s+/g, '');
    if (/^hex:/i.test(text)) {
        const buf = Buffer.from(text.replace(/^hex:/i, '').replace(/\s+/g, ''), 'hex');
        return tryExtractLoginBodyFromGate(buf) || { body: buf, encrypted: false };
    }
    if (/^base64:/i.test(text)) {
        const buf = Buffer.from(text.replace(/^base64:/i, '').replace(/\s+/g, ''), 'base64');
        return tryExtractLoginBodyFromGate(buf) || { body: buf, encrypted: false };
    }
    if (stripped && stripped.length % 2 === 0 && /^[0-9a-fA-F]+$/.test(stripped)) {
        const buf = Buffer.from(stripped, 'hex');
        return tryExtractLoginBodyFromGate(buf) || { body: buf, encrypted: false };
    }
    if (/\\x[0-9a-fA-F]{2}/.test(text)) {
        const buf = parseHexEscapes(text);
        return tryExtractLoginBodyFromGate(buf) || { body: buf, encrypted: false };
    }
    if (/^[A-Za-z0-9+/=]+$/.test(stripped) && stripped.length >= 16 && stripped.length % 4 === 0) {
        try {
            const buf = Buffer.from(stripped, 'base64');
            if (buf.length > 0) return tryExtractLoginBodyFromGate(buf) || { body: buf, encrypted: false };
        } catch (e) { }
    }

    return null;
}

function loadExternalLoginBody() {
    const configured = String(CONFIG.loginBodyFile || process.env.QQ_FARM_LOGIN_BODY_FILE || '').trim();
    if (!configured) return null;

    const filePath = path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
    if (!fs.existsSync(filePath)) {
        logWarn('登录', `原始登录 body 文件不存在: ${filePath}`);
        return null;
    }

    try {
        const raw = fs.readFileSync(filePath);
        const decoded = decodeExternalLoginPayload(raw);
        if (!decoded || !decoded.body || !decoded.body.length) {
            logWarn('登录', `原始登录 body 文件无法解析: ${filePath}`);
            return null;
        }
        return { body: Buffer.from(decoded.body), source: filePath, encrypted: !!decoded.encrypted };
    } catch (e) {
        logWarn('登录', `读取原始登录 body 失败: ${e.message}`);
        return null;
    }
}

function requestFatalStop(kind, detail = '') {
    if (fatalStopRequested) return;
    fatalStopRequested = true;
    reconnectScheduling = false;
    manualClosed = true;

    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    clearHeartbeatTimer();
    clearPendingCallbacks(`AUTO_STOP: ${kind}`);

    const msg = detail ? `${kind} | ${detail}` : kind;
    log('WS', `[AUTO_STOP] ${msg}`);

    const current = ws;
    ws = null;
    if (current) {
        try {
            current.removeAllListeners();
            current.terminate();
        } catch (e) { }
    }

    setTimeout(() => {
        try {
            process.kill(process.pid, 'SIGTERM');
        } catch (e) {
            process.exit(1);
        }
    }, 50);
}

function maybeHandleFatalGameError(serviceName, methodName, errorCode, errorMessage) {
    if (!Number.isFinite(errorCode) || errorCode <= 0) return;
    const service = String(serviceName || '');
    const method = String(methodName || '');
    const msg = String(errorMessage || '');
    const isKicked = errorCode === 1000014 || /其他地方登录|玩家已在其他地方登录/.test(msg);
    if (isKicked) {
        setReconnectStopReason('kicked', `${service}.${method} code=${errorCode} ${msg}`.trim());
        if (!fatalStopRequested) {
            scheduleReconnect(`kicked ${service}.${method} code=${errorCode}`, true);
        }
        return;
    }

    const isLoginMethod = /UserService$/.test(service) && method === 'Login';
    if (isLoginMethod) {
        const detail = `${service}.${method} code=${errorCode} ${msg}`.trim();
        const stopKind = (errorCode === 1020001 || /网络繁忙/.test(msg))
            ? 'login_failed'
            : 'code_expired';
        if (stopKind === 'login_failed' && CONFIG.platform === 'wx' && !activeLoginBodySource) {
            logWarn('登录', '微信登录当前需要加密后的请求体；已按 success-case 接入 wasm 加密，如仍失败再检查版本或 code 可用性');
        }
        setReconnectStopReason(stopKind, detail);
        // code is effectively one-shot; do not retry same code on login error
        if (!fatalStopRequested) {
            requestFatalStop(stopKind, detail);
        }
    }
}

async function encodeMsg(serviceName, methodName, bodyBytes, opts = {}) {
    let finalBody = bodyBytes || Buffer.alloc(0);
    if (finalBody.length > 0 && !opts.skipEncrypt) {
        finalBody = await cryptoWasm.encryptBuffer(finalBody);
    }
    const msg = types.GateMessage.create({
        meta: {
            service_name: serviceName,
            method_name: methodName,
            message_type: 1,
            client_seq: toLong(clientSeq),
            server_seq: toLong(serverSeq),
        },
        body: finalBody,
    });
    const encoded = types.GateMessage.encode(msg).finish();
    clientSeq++;
    return encoded;
}

async function sendMsg(serviceName, methodName, bodyBytes, callback, opts = {}) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        log('WS', '连接未打开');
        if (callback) callback(new Error('连接未打开'));
        return false;
    }
    const seq = clientSeq;
    let encoded;
    try {
        encoded = await encodeMsg(serviceName, methodName, bodyBytes, opts);
    } catch (e) {
        if (callback) callback(e);
        return false;
    }
    if (callback) pendingCallbacks.set(seq, callback);
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        if (callback) {
            pendingCallbacks.delete(seq);
            callback(new Error('连接在加密过程中关闭'));
        }
        return false;
    }
    try {
        ws.send(encoded);
        return true;
    } catch (e) {
        if (callback) pendingCallbacks.delete(seq);
        logWarn('WS', `发送失败: ${e.message}`);
        scheduleReconnect('send failed', true);
        return false;
    }
}

/** Promise 鐗堝彂閫?*/
function sleepMs(ms) {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms | 0)));
}

function resetAsyncSendThrottleState() {
    asyncSendPenaltyMs = 0;
    lastAsyncSendAt = 0;
    asyncSendQueue = Promise.resolve();
    asyncTimeoutStreak = 0;
    lastAsyncTimeoutAt = 0;
    lastAsyncReconnectAt = 0;
}

function getPendingPenaltyMs(pending) {
    if (pending <= 0) return 0;
    const idx = Math.min(pending, ASYNC_SEND_PENDING_PENALTY_MS.length - 1);
    return ASYNC_SEND_PENDING_PENALTY_MS[idx] || 0;
}

function getAsyncInFlightLimit() {
    // After a timeout, degrade immediately to single in-flight for stability.
    if (asyncTimeoutStreak > 0) return 1;
    return asyncSendPenaltyMs >= 80 ? 1 : 2;
}

async function waitForAsyncSendSlot() {
    while (true) {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;
        if (pendingCallbacks.size < getAsyncInFlightLimit()) return;
        await sleepMs(ASYNC_SEND_SLOT_WAIT_MS);
    }
}

function getCurrentAsyncSendGapMs() {
    const pending = pendingCallbacks.size;
    const dynamicGap = ASYNC_SEND_THROTTLE_BASE_GAP_MS
        + asyncSendPenaltyMs
        + getPendingPenaltyMs(pending);
    return Math.min(dynamicGap, ASYNC_SEND_THROTTLE_MAX_GAP_MS);
}

function getAsyncSendLoad() {
    return {
        pending: pendingCallbacks.size,
        penaltyMs: asyncSendPenaltyMs,
        timeoutStreak: asyncTimeoutStreak,
        inFlightLimit: getAsyncInFlightLimit(),
        dynamicGapMs: getCurrentAsyncSendGapMs(),
    };
}

async function applyAsyncSendThrottle() {
    await waitForAsyncSendSlot();
    const gap = getCurrentAsyncSendGapMs();
    const elapsed = Date.now() - lastAsyncSendAt;
    const waitMs = gap - elapsed;
    if (waitMs > 0) {
        await sleepMs(waitMs);
    }
    lastAsyncSendAt = Date.now();
}

function maybeScheduleReconnectOnAsyncTimeout() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (manualClosed || fatalStopRequested) return;
    if (reconnectTimer) return;
    if (asyncTimeoutStreak < ASYNC_TIMEOUT_STREAK_TRIGGER) return;

    const now = Date.now();
    if (now - lastAsyncReconnectAt < ASYNC_TIMEOUT_RECONNECT_COOLDOWN_MS) return;

    lastAsyncReconnectAt = now;
    asyncTimeoutStreak = 0;
    logWarn('WS', `async timeout streak reached, reconnect now`);
    scheduleReconnect('async timeout streak', true);
}

function recordAsyncSendResult({ timedOut = false, ok = false } = {}) {
    const now = Date.now();
    if (timedOut) {
        asyncSendPenaltyMs = Math.min(
            asyncSendPenaltyMs + ASYNC_SEND_TIMEOUT_PENALTY_STEP_MS,
            ASYNC_SEND_TIMEOUT_PENALTY_MAX_MS
        );
        if (now - lastAsyncTimeoutAt > ASYNC_TIMEOUT_STREAK_RESET_MS) {
            asyncTimeoutStreak = 0;
        }
        asyncTimeoutStreak += 1;
        lastAsyncTimeoutAt = now;
        maybeScheduleReconnectOnAsyncTimeout();
        return;
    }
    if (ok) {
        asyncSendPenaltyMs = Math.max(asyncSendPenaltyMs - ASYNC_SEND_RECOVERY_STEP_MS, 0);
        asyncTimeoutStreak = 0;
    }
}

function enqueueAsyncSend(task) {
    const run = () => Promise.resolve().then(task);
    const queued = asyncSendQueue.then(run, run);
    asyncSendQueue = queued.catch(() => {});
    return queued;
}

function sendMsgAsync(serviceName, methodName, bodyBytes, timeout = ASYNC_SEND_DEFAULT_TIMEOUT_MS) {
    return enqueueAsyncSend(async () => {
        // check connection
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            throw new Error(`连接未打开: ${methodName}`);
        }

        await applyAsyncSendThrottle();

        return new Promise((resolve, reject) => {
            const seq = clientSeq;
            let settled = false;
            const timeoutMs = Math.max(1000, Number(timeout) || ASYNC_SEND_DEFAULT_TIMEOUT_MS);
            const timer = setTimeout(() => {
                if (settled) return;
                settled = true;
                pendingCallbacks.delete(seq);
                const pending = pendingCallbacks.size;
                recordAsyncSendResult({ timedOut: true });
                reject(new Error(`request timeout: ${methodName} (seq=${seq}, pending=${pending})`));
            }, timeoutMs);

            sendMsg(serviceName, methodName, bodyBytes, (err, body, meta) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                if (err) {
                    const msg = sanitizeLogText((err && err.message) || err || '');
                    if (/timeout|request timeout|请求超时/i.test(msg)) {
                        recordAsyncSendResult({ timedOut: true });
                    }
                    if (err instanceof Error) {
                        err.message = msg;
                        reject(err);
                    } else {
                        reject(new Error(msg));
                    }
                    return;
                }
                recordAsyncSendResult({ ok: true });
                resolve({ body, meta });
            }).then((sent) => {
                if (sent || settled) return;
                settled = true;
                clearTimeout(timer);
                reject(new Error(`发送失败: ${methodName}`));
            }).catch((e) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                reject(e);
            });
        });
    });
}

// ============ 娑堟伅澶勭悊 ============
function handleMessage(data) {
    try {
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
        const msg = types.GateMessage.decode(buf);
        const meta = msg.meta;
        if (!meta) return;

        if (meta.server_seq) {
            const seq = toNum(meta.server_seq);
            if (seq > serverSeq) serverSeq = seq;
        }

        const msgType = meta.message_type;

        // Notify
        if (msgType === 3) {
            handleNotify(msg);
            return;
        }

        // Response
        if (msgType === 2) {
            lastHeartbeatResponse = Date.now();
            const errorCode = toNum(meta.error_code);
            const clientSeqVal = toNum(meta.client_seq);
            const errorMessage = sanitizeLogText(meta.error_message || '');
            maybeHandleFatalGameError(meta.service_name, meta.method_name, errorCode, errorMessage);

            const cb = pendingCallbacks.get(clientSeqVal);
            if (cb) {
                pendingCallbacks.delete(clientSeqVal);
                if (errorCode !== 0) {
                    const detail = `${meta.service_name}.${meta.method_name} 错误: code=${errorCode}${errorMessage ? ` ${errorMessage}` : ''}`;
                    cb(new Error(detail));
                } else {
                    cb(null, msg.body, meta);
                }
                return;
            }

            if (errorCode !== 0) {
                logWarn('错误', `${meta.service_name}.${meta.method_name} code=${errorCode}${errorMessage ? ` ${errorMessage}` : ''}`);
            }
        }
    } catch (err) {
        logWarn('解码', err.message);
    }
}

// 璋冭瘯锛氳褰曟墍鏈夋帹閫佺被鍨?(璁句负 true 鍙煡鐪嬫墍鏈夋帹閫?
// 娉ㄦ剰锛歈Q鐜涓嬪彧鏈?ItemNotify 鎺ㄩ€侊紝娌℃湁 LandsNotify 鎺ㄩ€?
const DEBUG_NOTIFY = false;

function handleNotify(msg) {
    if (!msg.body || msg.body.length === 0) return;
    try {
        const event = types.EventMessage.decode(msg.body);
        const type = event.message_type || '';
        const eventBody = event.body;

        // 璋冭瘯锛氭樉绀烘墍鏈夋帹閫佺被鍨?
        if (DEBUG_NOTIFY) {
            console.log('[DEBUG] 收到推送: ' + type);
        }

        // 琚涪涓嬬嚎
        if (type.includes('Kickout')) {
            log('PUSH', `kickout notify: ${type}`);
            try {
                const notify = types.KickoutNotify.decode(eventBody);
                log('PUSH', `kickout reason: ${notify.reason_message || 'unknown'}`);
            } catch (e) { }
            setReconnectStopReason('kicked', type);
            if (!fatalStopRequested) {
                scheduleReconnect(`kickout notify ${type}`, true);
            }
            return;
        }

        // 鍦熷湴鐘舵€佸彉鍖?(琚斁铏?鏀捐崏/鍋疯彍绛?
        if (type.includes('LandsNotify')) {
            try {
                const notify = types.LandsNotify.decode(eventBody);
                const hostGid = toNum(notify.host_gid);
                const lands = notify.lands || [];
                if (DEBUG_NOTIFY) {
                    console.log(`[DEBUG] LandsNotify: hostGid=${hostGid}, myGid=${userState.gid}, lands=${lands.length}`);
                }
                if (lands.length > 0) {
                    // 濡傛灉鏄嚜宸辩殑鍐滃満锛岃Е鍙戜簨浠?
                    if (hostGid === userState.gid || hostGid === 0) {
                        networkEvents.emit('landsChanged', lands);
                    }
                }
            } catch (e) { }
            return;
        }

        // 鐗╁搧鍙樺寲閫氱煡 (缁忛獙/金币绛? - 浠呮洿鏂扮姸鎬佹爮
        // 金币: id=1 鎴?id=1001 (GodItemId)
        // 缁忛獙: id=1101 (ExpItemId) 鎴?id=2
        if (type.includes('ItemNotify')) {
            try {
                const notify = types.ItemNotify.decode(eventBody);
                const items = notify.items || [];
                for (const itemChg of items) {
                    const item = itemChg.item;
                    if (!item) continue;
                    const id = toNum(item.id);
                    const count = toNum(item.count);
                    
                    if (id === 1101 || id === 2) {
                        userState.exp = count;
                        updateStatusLevel(userState.level, count);
                    } else if (id === 1 || id === 1001) {
                        userState.gold = count;
                        updateStatusGold(count);
                    }
                }
            } catch (e) { }
            return;
        }

        // 鍩烘湰淇℃伅鍙樺寲 (鍗囩骇绛?
        if (type.includes('BasicNotify')) {
            try {
                const notify = types.BasicNotify.decode(eventBody);
                if (notify.basic) {
                    const oldLevel = userState.level;
                    const oldExp = userState.exp || 0;
                    userState.level = toNum(notify.basic.level) || userState.level;
                    userState.gold = toNum(notify.basic.gold) || userState.gold;
                    const exp = toNum(notify.basic.exp);
                    if (exp > 0) {
                        userState.exp = exp;
                        updateStatusLevel(userState.level, exp);
                    }
                    updateStatusGold(userState.gold);
                    // 鍗囩骇鎻愮ず
                    if (userState.level !== oldLevel) {
                        log('系统', '升级! Lv' + oldLevel + ' -> Lv' + userState.level);
                        if (oldLevel > 0 && userState.level > oldLevel) {
                            networkEvents.emit('userLevelChanged', {
                                oldLevel,
                                newLevel: userState.level,
                                oldExp,
                                newExp: userState.exp || 0,
                            });
                        }
                    }
                }
            } catch (e) { }
            return;
        }

        // 濂藉弸鐢宠閫氱煡 (寰俊鍚岀帺)
        if (type.includes('FriendApplicationReceivedNotify')) {
            try {
                const notify = types.FriendApplicationReceivedNotify.decode(eventBody);
                const applications = notify.applications || [];
                if (applications.length > 0) {
                    networkEvents.emit('friendApplicationReceived', applications);
                }
            } catch (e) { }
            return;
        }

        // 濂藉弸娣诲姞鎴愬姛閫氱煡
        if (type.includes('FriendAddedNotify')) {
            try {
                const notify = types.FriendAddedNotify.decode(eventBody);
                const friends = notify.friends || [];
                if (friends.length > 0) {
                    const names = friends.map(f => f.name || f.remark || `GID:${toNum(f.gid)}`).join(', ');
                    log('好友', '新好友: ' + names);
                }
            } catch (e) { }
            return;
        }

        // 鐗╁搧鍙樺寲閫氱煡 (鏀惰幏/璐拱/娑堣€楃瓑)
        if (type.includes('ItemNotify')) {
            try {
                const notify = types.ItemNotify.decode(eventBody);
                const items = notify.items || [];
                for (const chg of items) {
                    if (!chg.item) continue;
                    const id = toNum(chg.item.id);
                    const count = toNum(chg.item.count);
                    const delta = toNum(chg.delta);
                    // 金币 ID=1
                    if (id === 1) {
                        userState.gold = count;
                        updateStatusGold(count);
                        if (delta !== 0) {
                            log('物品', '金币 ' + (delta > 0 ? '+' : '') + delta + ' (当前: ' + count + ')');
                        }
                    }
                    // 缁忛獙 ID=2 (鍗囩骇鐢?BasicNotify 澶勭悊)
                }
            } catch (e) { }
            return;
        }

        // 鍟嗗搧瑙ｉ攣閫氱煡 (鍗囩骇鍚庤В閿佹柊绉嶅瓙绛?
        if (type.includes('GoodsUnlockNotify')) {
            try {
                const notify = types.GoodsUnlockNotify.decode(eventBody);
                const goods = notify.goods_list || [];
                if (goods.length > 0) {
                    log('商店', '解锁 ' + goods.length + ' 个新品!');
                }
            } catch (e) { }
            return;
        }

        // 浠诲姟鐘舵€佸彉鍖栭€氱煡
        if (type.includes('TaskInfoNotify')) {
            try {
                const notify = types.TaskInfoNotify.decode(eventBody);
                if (notify.task_info) {
                    networkEvents.emit('taskInfoNotify', notify.task_info);
                }
            } catch (e) { }
            return;
        }

        // 鍏朵粬鏈鐞嗙殑鎺ㄩ€佺被鍨?(璋冭瘯鐢?
        // log('鎺ㄩ€?, `鏈鐞嗙被鍨? ${type}`);
    } catch (e) {
        logWarn('PUSH', `notify decode failed: ${e.message}`);
    }
}

// ============ 登录 ============
function sendLogin() {
    const externalLogin = loadExternalLoginBody();
    activeLoginBodySource = externalLogin ? externalLogin.source : '';
    const body = externalLogin
        ? externalLogin.body
        : types.LoginRequest.encode(types.LoginRequest.create({
            sharer_id: toLong(0),
            sharer_open_id: '',
            device_info: CONFIG.device_info,
            share_cfg_id: toLong(0),
            scene_id: CONFIG.loginSceneId,
            report_data: { ...CONFIG.loginReportData },
        })).finish();

    if (externalLogin) {
        log('登录', `使用原始登录 body: ${externalLogin.source}`);
    }

    sendMsg('gamepb.userpb.UserService', 'Login', body, (err, bodyBytes) => {
        if (err) {
            log('登录', '失败: ' + err.message);
            if (!fatalStopRequested) scheduleReconnect('login failed');
            return;
        }
        try {
            const reply = types.LoginReply.decode(bodyBytes);
            if (reply.basic) {
                userState.gid = toNum(reply.basic.gid);
                userState.name = reply.basic.name || '未知';
                userState.openId = reply.basic.open_id || '';
                userState.avatarUrl = reply.basic.avatar_url || '';
                userState.level = toNum(reply.basic.level);
                userState.gold = toNum(reply.basic.gold);
                userState.exp = toNum(reply.basic.exp);

                // 鏇存柊鐘舵€佹爮
                updateStatusFromLogin({
                    name: userState.name,
                    level: userState.level,
                    gold: userState.gold,
                    exp: userState.exp,
                });

                console.log('');
                console.log('========== 登录成功 ==========');
                console.log(`  GID:    ${userState.gid}`);
                console.log(`  昵称:   ${userState.name}`);
                console.log(`  等级:   ${userState.level}`);
                console.log(`  金币:   ${userState.gold}`);
                if (reply.time_now_millis) {
                    syncServerTime(toNum(reply.time_now_millis));
                    console.log(`  时间:   ${new Date(toNum(reply.time_now_millis)).toLocaleString()}`);
                }
                console.log('===============================');
                console.log('');
            }

            void (async () => {
                startHeartbeat();
                reconnectAttempts = 0;
                resetAsyncSendThrottleState();
                clearReconnectStopReason();

                try {
                    await runLoginWarmup({
                        sendMsgAsync,
                        types,
                        sleep: sleepMs,
                        profile: {
                            name: userState.name,
                            avatarUrl: userState.avatarUrl,
                        },
                        passiveSyncIntervalMs: CONFIG.passiveSessionSyncIntervalMs,
                    });
                } catch (e) {
                    logWarn('登录', `会话预热失败: ${e.message}`);
                }

                if (!initialLoginDone) {
                    initialLoginDone = true;
                    if (loginSuccessHandler) {
                        Promise.resolve(loginSuccessHandler()).catch((e) => {
                            logWarn('登录', `启动后初始化失败: ${e.message}`);
                        });
                    }
                } else {
                    log('WS', '重连成功，已恢复运行');
                }
            })();
        } catch (e) {
            log('登录', `解码失败: ${e.message}`);
            if (!fatalStopRequested) scheduleReconnect('login decode failed');
        }
    }, { skipEncrypt: !!(externalLogin && externalLogin.encrypted) }).then((sent) => {
        if (!sent && !fatalStopRequested) {
            scheduleReconnect('login send failed');
        }
    }).catch((e) => {
        logWarn('登录', `发送失败: ${e.message}`);
        if (!fatalStopRequested) scheduleReconnect('login send failed');
    });
}

// ============ 心跳 ============
let lastHeartbeatResponse = Date.now();
let heartbeatMissCount = 0;

function startHeartbeat() {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    lastHeartbeatResponse = Date.now();
    heartbeatMissCount = 0;
    
    heartbeatTimer = setInterval(() => {
        if (!userState.gid) return;
        
        // 妫€鏌ヤ笂娆″績璺冲搷搴旀椂闂达紝瓒呰繃 60 绉掓病鍝嶅簲璇存槑杩炴帴鏈夐棶棰?
        const timeSinceLastResponse = Date.now() - lastHeartbeatResponse;
        if (timeSinceLastResponse > 60000) {
            heartbeatMissCount++;
            logWarn('HB', `possible disconnect (${Math.round(timeSinceLastResponse / 1000)}s no response, pending=${pendingCallbacks.size})`);
            if (heartbeatMissCount >= 2) {
                log('HB', 'reconnect...');
                scheduleReconnect('heartbeat timeout', true);
            }
        }
        
        const body = types.HeartbeatRequest.encode(types.HeartbeatRequest.create({
            gid: toLong(userState.gid),
            client_version: CONFIG.clientVersion,
        })).finish();
        sendMsg('gamepb.userpb.UserService', 'Heartbeat', body, (err, replyBody) => {
            if (err || !replyBody) return;
            lastHeartbeatResponse = Date.now();
            heartbeatMissCount = 0;
            try {
                const reply = types.HeartbeatReply.decode(replyBody);
                if (reply.server_time) syncServerTime(toNum(reply.server_time));
            } catch (e) { }
        }).then((sent) => {
            if (!sent) {
                scheduleReconnect('heartbeat send failed', true);
            }
        }).catch(() => {
            scheduleReconnect('heartbeat send failed', true);
        });
    }, CONFIG.heartbeatInterval);
}

// ============ WebSocket 杩炴帴 ============
function buildConnectUrl(code, openID = '') {
    const qs = [
        `platform=${encodeURIComponent(CONFIG.platform)}`,
        `os=${encodeURIComponent(CONFIG.os)}`,
        `ver=${encodeURIComponent(CONFIG.clientVersion)}`,
        `code=${encodeURIComponent(code || '')}`,
        `openID=${encodeURIComponent(openID || '')}`,
    ].join('&');
    return `${CONFIG.serverUrl}?${qs}`;
}

function getWsHandshakeHeaders() {
    return {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Cache-Control': 'no-cache',
        'Origin': 'weapp://wechat-game-runtime',
        'Pragma': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    };
}

function scheduleReconnect(reason = 'unknown', immediate = false) {
    if (fatalStopRequested) return;
    if (manualClosed || !connectCode) return;
    if (reconnectTimer) return;
    if (reconnectScheduling) return;

    reconnectScheduling = true;
    try {
        clearHeartbeatTimer();
        reconnectAttempts++;
        if (reconnectAttempts > RECONNECT_MAX_RETRIES) {
            const stopKind = reconnectStopKind || 'reconnect_retry_exhausted';
            const baseDetail = reconnectStopDetail || String(reason || '');
            logWarn('WS', `reconnect retries exhausted (${RECONNECT_MAX_RETRIES}), stop running`);
            requestFatalStop(stopKind, `${baseDetail} | retries=${RECONNECT_MAX_RETRIES}`.trim());
            return;
        }
        const delay = immediate
            ? 0
            : Math.min(RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1), RECONNECT_MAX_DELAY);
        log('WS', `schedule reconnect (${reason}) ${reconnectAttempts}/${RECONNECT_MAX_RETRIES}, after ${delay}ms`);

        const current = ws;
        if (current) {
            try { current.terminate(); } catch (e) { }
        }
        ws = null;

        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            openSocket(true);
        }, delay);

        // Clear callbacks after reconnect timer is armed, so callback-side failures
        // won't recursively schedule reconnect and flood retry-exhausted logs.
        clearPendingCallbacks('连接超时，已清理');
    } finally {
        reconnectScheduling = false;
    }
}

function openSocket(isReconnect = false) {
    const url = buildConnectUrl(connectCode, connectOpenID);
    if (!isReconnect) {
        console.log("[WS] connect url =", url);
    } else {
        log('WS', `重连 url = ${url}`);
    }

    const socket = new WebSocket(url, {
        headers: getWsHandshakeHeaders(),
        perMessageDeflate: true,
    });

    ws = socket;
    socket.binaryType = 'arraybuffer';
    resetSeqState();

    socket.on('open', () => {
        if (socket !== ws) return;
        lastHeartbeatResponse = Date.now();
        heartbeatMissCount = 0;
        sendLogin();
    });

    socket.on('message', (data) => {
        if (socket !== ws) return;
        handleMessage(Buffer.isBuffer(data) ? data : Buffer.from(data));
    });

    socket.on('close', (code) => {
        if (socket !== ws) return;
        console.log(`[WS] 连接关闭 (code=${code})`);
        ws = null;
        clearHeartbeatTimer();
        clearPendingCallbacks('connection closed, callbacks cleared');
        if (!manualClosed) {
            scheduleReconnect(`close code=${code}`);
        }
    });

    socket.on('error', (err) => {
        if (socket !== ws) return;
        logWarn('WS', `错误: ${err.message}`);
        if (!manualClosed && socket.readyState !== WebSocket.OPEN) {
            scheduleReconnect(`error: ${err.message}`);
        }
    });
}

function connect(code, onLoginSuccess, openID = '') {
    manualClosed = false;
    fatalStopRequested = false;
    reconnectScheduling = false;
    activeLoginBodySource = '';
    clearReconnectStopReason();
    connectCode = code;
    connectOpenID = openID;
    loginSuccessHandler = onLoginSuccess;
    initialLoginDone = false;
    reconnectAttempts = 0;
    resetAsyncSendThrottleState();

    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }

    const current = ws;
    ws = null;
    if (current) {
        try {
            current.removeAllListeners();
            current.terminate();
        } catch (e) { }
    }

    openSocket(false);
}

function cleanup() {
    manualClosed = true;
    reconnectScheduling = false;
    activeLoginBodySource = '';
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    clearHeartbeatTimer();
    clearPendingCallbacks('client cleanup, callbacks cleared');
    resetAsyncSendThrottleState();

    const current = ws;
    ws = null;
    if (current) {
        try {
            current.removeAllListeners();
            current.close();
        } catch (e) { }
    }
}

function getWs() { return ws; }

module.exports = {
    connect, cleanup, getWs,
    sendMsg, sendMsgAsync,
    getAsyncSendLoad,
    getUserState,
    networkEvents,
};

