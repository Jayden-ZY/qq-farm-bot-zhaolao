/**
 * QQ经典农场 挂机脚本 - 入口文件
 *
 * 模块结构:
 *   src/config.js   - 配置常量与枚举
 *   src/utils.js    - 通用工具函数
 *   src/proto.js    - Protobuf 加载与类型管理
 *   src/network.js  - WebSocket 连接/消息编解码/登录/心跳
 *   src/farm.js     - 自己农场操作与巡田循环
 *   src/friend.js   - 好友农场操作与巡查循环
 *   src/decode.js   - PB解码/验证工具模式
 */

const fs = require('fs');
const path = require('path');
const { CONFIG } = require('./src/config');
const { loadProto } = require('./src/proto');
const { connect, cleanup, getWs } = require('./src/network');
const { startFarmCheckLoop, stopFarmCheckLoop } = require('./src/farm');
const { startFriendCheckLoop, stopFriendCheckLoop } = require('./src/friend');
const { initTaskSystem, cleanupTaskSystem } = require('./src/task');
const { initStatusBar, cleanupStatusBar, setStatusPlatform } = require('./src/status');
const { startSellLoop, stopSellLoop, debugSellFruits } = require('./src/warehouse');
const { processInviteCodes } = require('./src/invite');
const { verifyMode, decodeMode } = require('./src/decode');
const { emitRuntimeHint } = require('./src/utils');
const { getQQFarmCodeByScan } = require('./src/qqQrLogin');
const { initFileLogger } = require('./src/logger');

initFileLogger();

const LOCK_FILE = path.join(__dirname, '.bot.lock');
let lockAcquired = false;
let shuttingDown = false;

function pidExists(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (e) {
        return e.code === 'EPERM';
    }
}

function acquireSingleInstanceLock() {
    if (fs.existsSync(LOCK_FILE)) {
        try {
            const oldPid = parseInt(fs.readFileSync(LOCK_FILE, 'utf8').trim(), 10);
            if (Number.isInteger(oldPid) && oldPid > 0 && pidExists(oldPid)) {
                console.error(`[启动] 检测到已有实例运行 (pid=${oldPid})，请先停止旧进程，或使用 --allow-multi`);
                process.exit(1);
            }
        } catch (e) {
            // ignore stale lock parse error
        }
        try {
            fs.unlinkSync(LOCK_FILE);
        } catch (e) {
            // ignore stale lock cleanup error
        }
    }

    try {
        fs.writeFileSync(LOCK_FILE, String(process.pid), { flag: 'wx' });
        lockAcquired = true;
    } catch (e) {
        if (e.code === 'EEXIST') {
            console.error('[启动] 检测到已有实例运行，请先停止旧进程');
            process.exit(1);
        }
        throw e;
    }
}

function releaseSingleInstanceLock() {
    if (!lockAcquired) return;
    try {
        fs.unlinkSync(LOCK_FILE);
    } catch (e) {
        // ignore
    }
    lockAcquired = false;
}

function gracefulShutdown(exitCode = 0) {
    if (shuttingDown) return;
    shuttingDown = true;

    cleanupStatusBar();
    console.log('\n[退出] 正在断开...');
    stopFarmCheckLoop();
    stopFriendCheckLoop();
    cleanupTaskSystem();
    stopSellLoop();
    cleanup();

    const ws = getWs();
    if (ws) ws.close();
    releaseSingleInstanceLock();
    process.exit(exitCode);
}

// ============ 帮助信息 ============
function showHelp() {
    console.log(`
QQ经典农场 挂机脚本
====================

用法:
  node client.js --code <登录code> [--wx] [--interval <秒>] [--friend-interval <秒>] [--seed-id <ID>]
  node client.js --qr [--interval <秒>] [--friend-interval <秒>]
  node client.js --verify
  node client.js --decode <数据> [--hex] [--gate] [--type <消息类型>]

参数:
  --code              小程序 login() 返回的临时凭证
  --qr                启动后使用QQ扫码获取登录code（仅QQ平台）
  --wx                使用微信登录 (默认为QQ小程序)
  --interval          自己农场巡查完成后等待秒数
  --friend-interval   好友巡查完成后等待秒数
  --seed-id           指定种子ID，0表示自动策略
  --allow-multi       允许同目录下多开实例 (默认禁止，避免同号互踢)
  --verify            验证proto定义
  --decode            解码PB数据 (运行 --decode 无参数查看详细帮助)
`);
}

// ============ 参数解析 ============
function parseArgs(args) {
    const options = {
        code: '',
        qrLogin: false,
        allowMulti: false,
    };

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--code' && args[i + 1]) {
            options.code = args[++i];
        }
        if (args[i] === '--qr') {
            options.qrLogin = true;
        }
        if (args[i] === '--allow-multi') {
            options.allowMulti = true;
        }
        if (args[i] === '--wx') {
            CONFIG.platform = 'wx';
        }
        if (args[i] === '--interval' && args[i + 1]) {
            const sec = parseInt(args[++i], 10);
            CONFIG.farmCheckInterval = Math.max(sec, 1) * 1000;
        }
        if (args[i] === '--friend-interval' && args[i + 1]) {
            const sec = parseInt(args[++i], 10);
            CONFIG.friendCheckInterval = Math.max(sec, 1) * 1000;
        }
        if (args[i] === '--seed-id' && args[i + 1]) {
            const seedId = parseInt(args[++i], 10);
            if (Number.isInteger(seedId) && seedId >= 0) {
                CONFIG.targetSeedId = seedId;
            }
        }
    }

    return options;
}

// ============ 主函数 ============
async function main() {
    const args = process.argv.slice(2);
    let usedQrLogin = false;

    // 加载 proto 定义
    await loadProto();

    // 验证模式
    if (args.includes('--verify')) {
        await verifyMode();
        return;
    }

    // 解码模式
    if (args.includes('--decode')) {
        await decodeMode(args);
        return;
    }

    // 正常挂机模式
    const options = parseArgs(args);

    // QQ 平台支持扫码登录: 显式 --qr，或未传 --code 时自动触发
    if (!options.code && CONFIG.platform === 'qq' && (options.qrLogin || !args.includes('--code'))) {
        console.log('[扫码登录] 正在获取二维码...');
        options.code = await getQQFarmCodeByScan();
        usedQrLogin = true;
        console.log(`[扫码登录] 获取成功，code=${options.code.substring(0, 8)}...`);
    }

    if (!options.code) {
        if (CONFIG.platform === 'wx') {
            console.log('[参数] 微信模式仍需通过 --code 传入登录凭证');
        }
        showHelp();
        process.exit(1);
    }

    // 扫码阶段结束后清屏，避免状态栏覆盖二维码区域导致界面混乱
    if (usedQrLogin && process.stdout.isTTY) {
        process.stdout.write('\x1b[2J\x1b[H');
    }

    if (!options.allowMulti) {
        acquireSingleInstanceLock();
    }
    process.on('exit', releaseSingleInstanceLock);

    // 初始化状态栏
    initStatusBar();
    setStatusPlatform(CONFIG.platform);
    emitRuntimeHint(true);

    const platformName = CONFIG.platform === 'wx' ? '微信' : 'QQ';
    console.log(`[启动] ${platformName} code=${options.code.substring(0, 8)}... 农场${CONFIG.farmCheckInterval / 1000}s 好友${CONFIG.friendCheckInterval / 1000}s`);

    // 连接并登录，登录成功后启动各功能模块
    connect(options.code, async () => {
        // 处理邀请码 (仅微信环境)
        await processInviteCodes();

        startFarmCheckLoop();
        startFriendCheckLoop();
        initTaskSystem();

        // 启动时立即检查一次背包
        setTimeout(() => debugSellFruits(), 5000);
        startSellLoop(60000);  // 每分钟自动出售仓库果实
    });

    // 退出处理
    process.on('SIGINT', () => gracefulShutdown(0));
    process.on('SIGTERM', () => gracefulShutdown(0));
}

main().catch((err) => {
    console.error('启动失败:', err);
    releaseSingleInstanceLock();
    process.exit(1);
});
