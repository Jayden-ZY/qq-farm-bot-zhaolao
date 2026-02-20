/**
 * 仓库系统 - 自动出售果实
 * 协议说明：BagReply 使用 item_bag（ItemBag），item_bag.items 才是背包物品列表
 */

const { types } = require('./proto');
const { sendMsgAsync, getUserState } = require('./network');
const { toLong, toNum, log, logWarn, sleep, emitRuntimeHint } = require('./utils');
const { getFruitName, getPlantByFruitId, getLevelExpProgress } = require('./gameConfig');
const seedShopData = require('../tools/seed-shop-merged-export.json');

// 游戏内金币和点券的物品 ID (GlobalData.GodItemId / DiamondItemId)
const GOLD_ITEM_ID = 1001;

// 单次 Sell 请求最多条数，过多可能触发 1000020 参数错误
const SELL_BATCH_SIZE = 15;

const FRUIT_ID_SET = new Set(
    ((seedShopData && seedShopData.rows) || [])
        .map((row) => Number(row.fruitId))
        .filter(Number.isFinite)
);

let sellTimer = null;
let sellInterval = 60000;

function isFruitItemId(id) {
    const n = toNum(id);
    if (FRUIT_ID_SET.has(n)) return true;
    // 兜底：种子类通常在 2xxxx，果实在 4xxxx
    if (n < 40000) return false;
    return !!getPlantByFruitId(n);
}

/**
 * 从 SellReply 中提取获得的金币数量
 * 新版 SellReply 返回 get_items (repeated Item)，其中 id=1001 为金币
 */
function extractGold(sellReply) {
    if (sellReply.get_items && sellReply.get_items.length > 0) {
        for (const item of sellReply.get_items) {
            const id = toNum(item.id);
            if (id === GOLD_ITEM_ID) {
                return toNum(item.count);
            }
        }
        return 0;
    }
    if (sellReply.gold !== undefined && sellReply.gold !== null) {
        return toNum(sellReply.gold);
    }
    return 0;
}

async function getBag() {
    const body = types.BagRequest.encode(types.BagRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.itempb.ItemService', 'Bag', body);
    return types.BagReply.decode(replyBody);
}

/**
 * 将 item 转为 Sell 请求所需格式（id/count/uid 保留 Long 或转成 Long，与游戏一致）
 */
function toSellItem(item) {
    const id = item.id != null ? toLong(item.id) : undefined;
    const count = item.count != null ? toLong(item.count) : undefined;
    const uid = item.uid != null ? toLong(item.uid) : undefined;
    return { id, count, uid };
}

async function sellItems(items) {
    const payload = items.map(toSellItem);
    const body = types.SellRequest.encode(types.SellRequest.create({ items: payload })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.itempb.ItemService', 'Sell', body);
    return types.SellReply.decode(replyBody);
}

/**
 * 从 BagReply 取出物品列表（兼容 item_bag 与旧版 items）
 */
function getBagItems(bagReply) {
    if (bagReply.item_bag && bagReply.item_bag.items && bagReply.item_bag.items.length)
        return bagReply.item_bag.items;
    return bagReply.items || [];
}

function getGoldFromItems(items) {
    for (const item of items) {
        if (toNum(item.id) === 1) return toNum(item.count);
    }
    return -1;
}

async function printTotalGold(prefetchedItems = null) {
    try {
        const items = prefetchedItems || getBagItems(await getBag());
        let gold = getGoldFromItems(items);
        if (gold < 0) {
            // 部分环境 Bag 不返回金币项，回退到内存态金币
            gold = toNum(getUserState().gold);
        }
        if (gold >= 0) {
            const state = getUserState();
            const level = toNum(state.level);
            const totalExp = toNum(state.exp);
            const progress = getLevelExpProgress(level, totalExp);

            if (progress.needed > 0) {
                const needToLevelUp = Math.max(0, progress.needed - progress.current);
                log('仓库', `总金币 ${gold} | Lv${level} 经验 ${progress.current}/${progress.needed} (升级还需${needToLevelUp})`);
            } else {
                log('仓库', `总金币 ${gold} | Lv${level}`);
            }
        }
    } catch (e) {
        logWarn('仓库', `总金币查询失败: ${e.message}`);
    }
}

async function sellAllFruits() {
    let totalPrinted = false;
    try {
        const bagReply = await getBag();
        const items = getBagItems(bagReply);

        const toSell = [];
        const soldSummary = new Map(); // name -> total count

        for (const item of items) {
            const id = toNum(item.id);
            const count = toNum(item.count);
            const uid = item.uid ? toNum(item.uid) : 0;

            if (!isFruitItemId(id) || count <= 0) continue;

            if (uid === 0) {
                logWarn('仓库', `跳过无效物品: ID=${id} Count=${count} (UID丢失)`);
                continue;
            }

            toSell.push(item);
            const name = getFruitName(id);
            soldSummary.set(name, (soldSummary.get(name) || 0) + count);
        }

        if (toSell.length === 0) {
            await printTotalGold(items);
            totalPrinted = true;
            return;
        }

        let gainedGold = 0;
        for (let i = 0; i < toSell.length; i += SELL_BATCH_SIZE) {
            const batch = toSell.slice(i, i + SELL_BATCH_SIZE);
            const reply = await sellItems(batch);
            gainedGold += extractGold(reply);
            if (i + SELL_BATCH_SIZE < toSell.length) await sleep(300);
        }

        const parts = Array.from(soldSummary.entries()).map(([name, count]) => `${name}x${count}`);
        if (gainedGold > 0) {
            log('仓库', `出售 ${parts.join(', ')}，获得 ${gainedGold} 金币`);
        } else {
            log('仓库', `出售 ${parts.join(', ')}`);
        }

        emitRuntimeHint(false);
        await printTotalGold();
        totalPrinted = true;
    } catch (e) {
        logWarn('仓库', `出售失败: ${e.message}`);
    } finally {
        if (!totalPrinted) {
            await printTotalGold();
        }
    }
}

async function debugSellFruits() {
    try {
        log('仓库', '正在检查背包...');
        const bagReply = await getBag();
        const items = getBagItems(bagReply);
        log('仓库', `背包共 ${items.length} 种物品`);

        for (const item of items) {
            const id = toNum(item.id);
            const count = toNum(item.count);
            if (isFruitItemId(id)) {
                const name = getFruitName(id);
                log('仓库', `  [果实] ${name}(${id}) x${count}`);
            }
        }

        await printTotalGold(items);
    } catch (e) {
        logWarn('仓库', `调试出售失败: ${e.message}`);
        console.error(e);
    }
}

function startSellLoop(interval = 60000) {
    if (sellTimer) return;
    sellInterval = interval;
    setTimeout(() => {
        sellAllFruits();
        sellTimer = setInterval(() => sellAllFruits(), sellInterval);
    }, 10000);
}

function stopSellLoop() {
    if (sellTimer) {
        clearInterval(sellTimer);
        sellTimer = null;
    }
}

module.exports = {
    getBag,
    sellItems,
    sellAllFruits,
    debugSellFruits,
    getBagItems,
    startSellLoop,
    stopSellLoop,
};
