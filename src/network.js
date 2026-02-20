/**
 * WebSocket 网络层 - 连接/消息编解码/登录/心跳
 */

const WebSocket = require('ws');
const EventEmitter = require('events');
const { CONFIG } = require('./config');
const { types } = require('./proto');
const { toLong, toNum, syncServerTime, log, logWarn } = require('./utils');
const { updateStatusFromLogin, updateStatusGold, updateStatusLevel } = require('./status');

// ============ 事件发射器 (用于推送通知) ============
const networkEvents = new EventEmitter();

// ============ 内部状态 ============
let ws = null;
let clientSeq = 1;
let serverSeq = 0;
let heartbeatTimer = null;
let pendingCallbacks = new Map();
let reconnectTimer = null;
let reconnectAttempts = 0;
let manualClosed = false;
let connectCode = '';
let connectOpenID = '';
let loginSuccessHandler = null;
let initialLoginDone = false;

const RECONNECT_BASE_DELAY = 2000;
const RECONNECT_MAX_DELAY = 30000;

// ============ 用户状态 (登录后设置) ============
const userState = {
    gid: 0,
    name: '',
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

// ============ 消息编解码 ============
function encodeMsg(serviceName, methodName, bodyBytes) {
    const msg = types.GateMessage.create({
        meta: {
            service_name: serviceName,
            method_name: methodName,
            message_type: 1,
            client_seq: toLong(clientSeq),
            server_seq: toLong(serverSeq),
        },
        body: bodyBytes || Buffer.alloc(0),
    });
    const encoded = types.GateMessage.encode(msg).finish();
    clientSeq++;
    return encoded;
}

function sendMsg(serviceName, methodName, bodyBytes, callback) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        log('WS', '连接未打开');
        return false;
    }
    const seq = clientSeq;
    const encoded = encodeMsg(serviceName, methodName, bodyBytes);
    if (callback) pendingCallbacks.set(seq, callback);
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

/** Promise 版发送 */
function sendMsgAsync(serviceName, methodName, bodyBytes, timeout = 10000) {
    return new Promise((resolve, reject) => {
        // 检查连接状态
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            reject(new Error(`连接未打开: ${methodName}`));
            return;
        }
        
        const seq = clientSeq;
        const timer = setTimeout(() => {
            pendingCallbacks.delete(seq);
            // 检查当前待处理的请求数
            const pending = pendingCallbacks.size;
            reject(new Error(`请求超时: ${methodName} (seq=${seq}, pending=${pending})`));
        }, timeout);

        const sent = sendMsg(serviceName, methodName, bodyBytes, (err, body, meta) => {
            clearTimeout(timer);
            if (err) reject(err);
            else resolve({ body, meta });
        });
        
        if (!sent) {
            clearTimeout(timer);
            reject(new Error(`发送失败: ${methodName}`));
        }
    });
}

// ============ 消息处理 ============
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

            const cb = pendingCallbacks.get(clientSeqVal);
            if (cb) {
                pendingCallbacks.delete(clientSeqVal);
                if (errorCode !== 0) {
                    cb(new Error(`${meta.service_name}.${meta.method_name} 错误: code=${errorCode} ${meta.error_message || ''}`));
                } else {
                    cb(null, msg.body, meta);
                }
                return;
            }

            if (errorCode !== 0) {
                logWarn('错误', `${meta.service_name}.${meta.method_name} code=${errorCode} ${meta.error_message || ''}`);
            }
        }
    } catch (err) {
        logWarn('解码', err.message);
    }
}

// 调试：记录所有推送类型 (设为 true 可查看所有推送)
// 注意：QQ环境下只有 ItemNotify 推送，没有 LandsNotify 推送
const DEBUG_NOTIFY = false;

function handleNotify(msg) {
    if (!msg.body || msg.body.length === 0) return;
    try {
        const event = types.EventMessage.decode(msg.body);
        const type = event.message_type || '';
        const eventBody = event.body;

        // 调试：显示所有推送类型
        if (DEBUG_NOTIFY) {
            console.log(`[DEBUG] 收到推送: ${type}`);
        }

        // 被踢下线
        if (type.includes('Kickout')) {
            log('推送', `被踢下线! ${type}`);
            try {
                const notify = types.KickoutNotify.decode(eventBody);
                log('推送', `原因: ${notify.reason_message || '未知'}`);
            } catch (e) { }
            return;
        }

        // 土地状态变化 (被放虫/放草/偷菜等)
        if (type.includes('LandsNotify')) {
            try {
                const notify = types.LandsNotify.decode(eventBody);
                const hostGid = toNum(notify.host_gid);
                const lands = notify.lands || [];
                if (DEBUG_NOTIFY) {
                    console.log(`[DEBUG] LandsNotify: hostGid=${hostGid}, myGid=${userState.gid}, lands=${lands.length}`);
                }
                if (lands.length > 0) {
                    // 如果是自己的农场，触发事件
                    if (hostGid === userState.gid || hostGid === 0) {
                        networkEvents.emit('landsChanged', lands);
                    }
                }
            } catch (e) { }
            return;
        }

        // 物品变化通知 (经验/金币等) - 仅更新状态栏
        // 金币: id=1 或 id=1001 (GodItemId)
        // 经验: id=1101 (ExpItemId) 或 id=2
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

        // 基本信息变化 (升级等)
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
                    // 升级提示
                    if (userState.level !== oldLevel) {
                        log('系统', `升级! Lv${oldLevel} → Lv${userState.level}`);
                    }
                }
            } catch (e) { }
            return;
        }

        // 好友申请通知 (微信同玩)
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

        // 好友添加成功通知
        if (type.includes('FriendAddedNotify')) {
            try {
                const notify = types.FriendAddedNotify.decode(eventBody);
                const friends = notify.friends || [];
                if (friends.length > 0) {
                    const names = friends.map(f => f.name || f.remark || `GID:${toNum(f.gid)}`).join(', ');
                    log('好友', `新好友: ${names}`);
                }
            } catch (e) { }
            return;
        }

        // 物品变化通知 (收获/购买/消耗等)
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
                            log('物品', `金币 ${delta > 0 ? '+' : ''}${delta} (当前: ${count})`);
                        }
                    }
                    // 经验 ID=2 (升级由 BasicNotify 处理)
                }
            } catch (e) { }
            return;
        }

        // 商品解锁通知 (升级后解锁新种子等)
        if (type.includes('GoodsUnlockNotify')) {
            try {
                const notify = types.GoodsUnlockNotify.decode(eventBody);
                const goods = notify.goods_list || [];
                if (goods.length > 0) {
                    log('商店', `解锁 ${goods.length} 个新商品!`);
                }
            } catch (e) { }
            return;
        }

        // 任务状态变化通知
        if (type.includes('TaskInfoNotify')) {
            try {
                const notify = types.TaskInfoNotify.decode(eventBody);
                if (notify.task_info) {
                    networkEvents.emit('taskInfoNotify', notify.task_info);
                }
            } catch (e) { }
            return;
        }

        // 其他未处理的推送类型 (调试用)
        // log('推送', `未处理类型: ${type}`);
    } catch (e) {
        logWarn('推送', `解码失败: ${e.message}`);
    }
}

// ============ 登录 ============
function sendLogin() {
    const body = types.LoginRequest.encode(types.LoginRequest.create({
        sharer_id: toLong(0),
        sharer_open_id: '',
        device_info: CONFIG.device_info,
        share_cfg_id: toLong(0),
        scene_id: '1256',
        report_data: {
            callback: '', cd_extend_info: '', click_id: '', clue_token: '',
            minigame_channel: 'other', minigame_platid: 2, req_id: '', trackid: '',
        },
    })).finish();

    const sent = sendMsg('gamepb.userpb.UserService', 'Login', body, (err, bodyBytes) => {
        if (err) {
            log('登录', `失败: ${err.message}`);
            scheduleReconnect('login failed');
            return;
        }
        try {
            const reply = types.LoginReply.decode(bodyBytes);
            if (reply.basic) {
                userState.gid = toNum(reply.basic.gid);
                userState.name = reply.basic.name || '未知';
                userState.level = toNum(reply.basic.level);
                userState.gold = toNum(reply.basic.gold);
                userState.exp = toNum(reply.basic.exp);

                // 更新状态栏
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

            startHeartbeat();
            reconnectAttempts = 0;
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
        } catch (e) {
            log('登录', `解码失败: ${e.message}`);
            scheduleReconnect('login decode failed');
        }
    });

    if (!sent) {
        scheduleReconnect('login send failed');
    }
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
        
        // 检查上次心跳响应时间，超过 60 秒没响应说明连接有问题
        const timeSinceLastResponse = Date.now() - lastHeartbeatResponse;
        if (timeSinceLastResponse > 60000) {
            heartbeatMissCount++;
            logWarn('心跳', `连接可能已断开 (${Math.round(timeSinceLastResponse/1000)}s 无响应, pending=${pendingCallbacks.size})`);
            if (heartbeatMissCount >= 2) {
                log('心跳', '尝试重连...');
                scheduleReconnect('heartbeat timeout', true);
            }
        }
        
        const body = types.HeartbeatRequest.encode(types.HeartbeatRequest.create({
            gid: toLong(userState.gid),
            client_version: CONFIG.clientVersion,
        })).finish();
        const sent = sendMsg('gamepb.userpb.UserService', 'Heartbeat', body, (err, replyBody) => {
            if (err || !replyBody) return;
            lastHeartbeatResponse = Date.now();
            heartbeatMissCount = 0;
            try {
                const reply = types.HeartbeatReply.decode(replyBody);
                if (reply.server_time) syncServerTime(toNum(reply.server_time));
            } catch (e) { }
        });
        if (!sent) {
            scheduleReconnect('heartbeat send failed', true);
        }
    }, CONFIG.heartbeatInterval);
}

// ============ WebSocket 连接 ============
function buildConnectUrl(code, openID = '') {
    return openID
        ? `${CONFIG.serverUrl}?platform=${CONFIG.platform}&os=${CONFIG.os}&ver=${CONFIG.clientVersion}&code=${code}&openID=${openID}`
        : `${CONFIG.serverUrl}?platform=${CONFIG.platform}&os=${CONFIG.os}&ver=${CONFIG.clientVersion}&code=${code}`;
}

function scheduleReconnect(reason = 'unknown', immediate = false) {
    if (manualClosed || !connectCode) return;
    if (reconnectTimer) return;

    clearHeartbeatTimer();
    clearPendingCallbacks('连接超时，已清理');

    reconnectAttempts++;
    const delay = immediate
        ? 0
        : Math.min(RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1), RECONNECT_MAX_DELAY);
    log('WS', `准备重连 (${reason})，${delay}ms 后重试`);

    const current = ws;
    if (current) {
        try { current.terminate(); } catch (e) { }
    }
    ws = null;

    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        openSocket(true);
    }, delay);
}

function openSocket(isReconnect = false) {
    const url = buildConnectUrl(connectCode, connectOpenID);
    if (!isReconnect) {
        console.log("[WS] connect url =", url);
    } else {
        log('WS', `重连 url = ${url}`);
    }

    const socket = new WebSocket(url, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090a13)',
            'Origin': 'https://gate-obt.nqf.qq.com',
        },
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
        clearPendingCallbacks('连接已关闭，已清理');
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
    connectCode = code;
    connectOpenID = openID;
    loginSuccessHandler = onLoginSuccess;
    initialLoginDone = false;
    reconnectAttempts = 0;

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
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    clearHeartbeatTimer();
    clearPendingCallbacks('客户端退出，已清理');

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
    getUserState,
    networkEvents,
};
