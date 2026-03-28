/**
 * 自己的农场操作 - 收获/浇水/除草/除虫/铲除/种植/商店/巡田
 */

const protobuf = require('protobufjs');
const fs = require('fs');
const path = require('path');
const { CONFIG, PlantPhase, PHASE_NAMES } = require('./config');
const { types } = require('./proto');
const { sendMsgAsync, getUserState, networkEvents, getAsyncSendLoad } = require('./network');
const { toLong, toNum, getServerTimeSec, toTimeSec, log, logWarn, sleep, sanitizeLogText } = require('./utils');
const { updateStatusGold } = require('./status');
const {
    getPlantNameBySeedId,
    getPlantName,
    getPlantExp,
    formatGrowTime,
    getPlantGrowTime,
    getItemInfoById,
    getItemName,
    isSeedItemId,
    getPlantBySeedId,
    getPlantByFruitId,
} = require('./gameConfig');
const { getPlantingRecommendation } = require('../tools/calc-exp-yield');
const { scheduleIllustratedClaim } = require('./task');

const _dataDir = process.env.QQ_FARM_DATA_DIR || null;
const WEBUI_CONFIG_PATH = _dataDir
    ? path.join(_dataDir, 'config.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/config.json');
const WEBUI_LANDS_PATH = _dataDir
    ? path.join(_dataDir, 'lands.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/lands.json');
const WEBUI_BAG_ITEMS_PATH = _dataDir
    ? path.join(_dataDir, 'bag-items.json')
    : path.resolve(__dirname, '../../qq-farm-webui/data/bag-items.json');
let webuiConfigMtimeMs = -1;
let webuiConfigCache = null;
let lastSeedFallbackKey = '';
let lastWebuiLandsWriteErrorMsg = '';
let lastWebuiLandsWriteErrorAt = 0;
const LEVEL_UP_RECOMMEND_FALLBACK_LANDS = 24;

const BENIGN_ACTION_ERROR_CODES = {
    浇水: new Set([1001012]),
    除草: new Set([1001015]),
    除虫: new Set([1001018]),
};

const LAND_UPGRADE_ERROR_LABELS = new Map([
    [1001001, '土地未解锁'],
    [1001006, '前置土地数量不足'],
    [1001007, '种植等级不足'],
    [1000019, '金币不足'],
]);

// ============ 内部状态 ============
let isCheckingFarm = false;
let isFirstFarmCheck = true;
let farmCheckTimer = null;
let farmLoopRunning = false;
let lastFertStatusLogAt = 0;
let pendingLandUpgradeSweep = false;
let pendingLandUpgradeSweepReason = '';
let isLandUpgradeSweepRunning = false;
let lastHandledLandUpgradeSweepRequestId = '';
let pendingMallDailyClaim = false;
let pendingMallDailyClaimReason = '';
let lastHandledMallDailyClaimRequestId = '';
let pendingBuy10hFert = false;
let pendingBuy10hFertReason = '';
let pendingBuy10hFertCount = 1;
let lastHandledBuy10hFertRequestId = '';
let pendingUseAllBagItems = false;
let pendingUseAllBagItemsReason = '';
let lastHandledUseAllBagItemsRequestId = '';
let pendingBagSnapshot = false;
let pendingBagSnapshotReason = '';
let lastHandledBagSnapshotRequestId = '';
let pendingUseSelectedBagItems = false;
let pendingUseSelectedBagItemsReason = '';
let pendingUseSelectedBagItemsItems = [];
let lastHandledUseSelectedBagItemsRequestId = '';
let startupMallDailyClaimQueued = false;
let lastMallDailyAutoClaimDateKey = '';
let farmFastHarvestEnabled = true;
let farmAutoFertilizeEnabled = true;
let lastKnownNormalFertilizerSec = null;
let lastKnownNormalFertilizerAt = 0;
let lastKnownDiamondCount = null;
let lastKnownDiamondAt = 0;
let lastNormalFertilizerAutoBuyAttemptAt = 0;
let lastNormalFertilizerLowLogAt = 0;
const fastHarvestTimers = new Map(); // landId -> { timer, matureTime, plantName }
const FERT_STATUS_CHECK_INTERVAL_MS = 5 * 60 * 1000; // 每 5 分钟主动上报一次肥料状态
const LAND_UPGRADE_MANUAL_REQUEST_MAX_AGE_MS = 60 * 60 * 1000;
const LOOP_TIMEOUT_BACKOFF_BASE_MS = 1200;
const LOOP_TIMEOUT_BACKOFF_MAX_MS = 8000;
const FARM_MIN_LOOP_WAIT_WHEN_ZERO_MS = 460;
const FARM_NETWORK_GUARD_MAX_MS = 2200;
const FAST_HARVEST_PREPARE_WINDOW_SEC = 60;
const FAST_HARVEST_EARLY_TRIGGER_MS = 200;
const FARM_UNLOCK_LAND_MAX_ID = 24;
const LAND_UNLOCK_REQUEST_SOURCE = 0; // 抓包解密样本为 080c1000 / 080d1000，UnlockLand 请求第二字段固定为 0
const LAND_GRID_COLUMNS = 4;
const LARGE_CROP_SIZE = 2;
const KNOWN_LARGE_SEED_IDS = new Set([20046, 20092, 20219, 29998]);
const KNOWN_LARGE_SEED_NAMES = new Set(['哈哈南瓜', '路易十四', '爱心果', '稀世琉璃果']);
const DIAMOND_ITEM_IDS = new Set([2, 1002]);
const NORMAL_FERTILIZER_BAG_USE_ITEM_SECONDS = new Map([
    [80001, 1 * 60 * 60],
    [80002, 4 * 60 * 60],
    [80003, 8 * 60 * 60],
    [80004, 12 * 60 * 60],
    [100003, 1 * 60 * 60], // 化肥礼包：按 1 小时普通化肥估算，下一轮会继续消化礼包产物
]);
const MALL_GOODS_DAILY_FREE_ID = 1001; // 每日福利
const MALL_GOODS_FERT_10H_NORMAL_ID = 1003; // 10小时化肥
const MALL_PRICE_FERT_10H_NORMAL = 34; // 抓包实测单价（点券）
const NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC = 10 * 60 * 60; // 低于 10 小时时尝试补充
const NORMAL_FERTILIZER_STATUS_REFRESH_INTERVAL_MS = 60 * 1000;
const NORMAL_FERTILIZER_AUTO_BUY_COOLDOWN_MS = 60 * 1000;
const NORMAL_FERTILIZER_LOW_LOG_INTERVAL_MS = 5 * 60 * 1000;
const NORMAL_FERTILIZER_AUTO_USE_MAX_ROUNDS = 4;
const SHARE_DAILY_ACTION_FLAG = 1; // 分享场景标记（抓包实测固定为 1）
const MALL_DEFAULT_BUY_10H_COUNT = 1;
const MALL_MAX_BUY_10H_COUNT = 999;
const MALL_DAILY_AUTO_HOUR = 0;
const MALL_DAILY_AUTO_MINUTE = 5;
const BAG_AUTO_USE_DIRECT_ITEM_ID_MIN = 80000; // 常规可直接 Use/BatchUse 的道具（化肥等）
const BAG_AUTO_USE_DIRECT_ITEM_ID_MAX = 89999;
const BAG_AUTO_USE_MAX_ROUNDS = 6;
const BAG_AUTO_USE_BATCH_SIZE = 8;
const BAG_AUTO_USE_SKIP_IDS = new Set([1, 2, 1001, 1002, 90004, 90005, 90006]); // 金币/点券/狗粮
const WAREHOUSE_HIDDEN_IDS = new Set([
    1, 2, 1001, 1002, 1003, 1004, 1005, 1011, 1012, 1013, 1014, 1015, 1101, 3001, 3002, 4001,
]);
const WAREHOUSE_CATEGORY_FRUITS = 'fruits';
const WAREHOUSE_CATEGORY_SUPER_FRUITS = 'superfruits';
const WAREHOUSE_CATEGORY_SEEDS = 'seeds';
const WAREHOUSE_CATEGORY_PROPS = 'props';
const WAREHOUSE_ITEM_TYPE_SEED = 5;
const WAREHOUSE_ITEM_TYPE_FRUIT = 6;
const WAREHOUSE_SUPER_FRUIT_ID_OFFSET = 1000000;
const WAREHOUSE_MUTATION_LABELS = new Map([
    [1, '冰冻'],
    [2, '湿润'],
    [3, '爱心'],
]);
const LARGE_SEED_PLANT_FAILURE_TTL_MS = 10 * 60 * 1000;
let farmLoopTimeoutBackoffMs = 0;
const runtimeLargeCropNames = new Set();
const largeSeedPlantFailureCache = new Map();

function getFarmNetworkGuardWaitMs() {
    const load = getAsyncSendLoad();
    if (!load) return 0;

    const pending = Math.max(0, Number(load.pending) || 0);
    const penaltyMs = Math.max(0, Number(load.penaltyMs) || 0);
    const timeoutStreak = Math.max(0, Number(load.timeoutStreak) || 0);
    const dynamicGapMs = Math.max(0, Number(load.dynamicGapMs) || 0);

    if (pending <= 0 && penaltyMs <= 0 && timeoutStreak <= 0) return 0;

    const guardMs = Math.max(
        Math.floor(dynamicGapMs * 2),
        penaltyMs + pending * 280 + timeoutStreak * 420
    );
    return Math.min(guardMs, FARM_NETWORK_GUARD_MAX_MS);
}

// ============ 农场 API ============

// 操作限制更新回调 (由 friend.js 设置)
let onOperationLimitsUpdate = null;
function setOperationLimitsCallback(callback) {
    onOperationLimitsUpdate = callback;
}

async function getAllLands() {
    const body = types.AllLandsRequest.encode(types.AllLandsRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'AllLands', body);
    const reply = types.AllLandsReply.decode(replyBody);
    // 更新操作限制
    if (reply.operation_limits && onOperationLimitsUpdate) {
        onOperationLimitsUpdate(reply.operation_limits);
    }
    return reply;
}

async function upgradeLand(landId) {
    const body = types.UpgradeLandRequest.encode(types.UpgradeLandRequest.create({
        land_id: toLong(landId),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'UpgradeLand', body);
    const reply = types.UpgradeLandReply.decode(replyBody);
    if (reply.operation_limits && onOperationLimitsUpdate) {
        onOperationLimitsUpdate(reply.operation_limits);
    }
    return reply;
}

function encodeUnlockLandRequestBody(landId) {
    const writer = protobuf.Writer.create();
    writer.uint32(8).int64(toLong(landId));
    writer.uint32(16).int64(toLong(LAND_UNLOCK_REQUEST_SOURCE));
    return writer.finish();
}

function normalizeUpgradeErrorText(err) {
    return sanitizeLogText(String((err && err.message) || err || '')).replace(/\s+/g, ' ').trim();
}

async function unlockLand(landId) {
    const body = encodeUnlockLandRequestBody(landId);
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'UnlockLand', body);
    // UnlockLand 的回包结构与 UpgradeLandReply 高度相似；解码失败时忽略，不影响主流程
    try {
        const reply = types.UpgradeLandReply.decode(replyBody);
        if (reply.operation_limits && onOperationLimitsUpdate) {
            onOperationLimitsUpdate(reply.operation_limits);
        }
    } catch (e) {
        // ignore decode mismatch
    }
}

function getBagItemsFromReply(bagReply) {
    if (bagReply && bagReply.item_bag && Array.isArray(bagReply.item_bag.items) && bagReply.item_bag.items.length > 0) {
        return bagReply.item_bag.items;
    }
    return (bagReply && Array.isArray(bagReply.items)) ? bagReply.items : [];
}

async function fetchBagItems() {
    const body = types.BagRequest.encode(types.BagRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.itempb.ItemService', 'Bag', body);
    const bagReply = types.BagReply.decode(replyBody);
    return getBagItemsFromReply(bagReply);
}

function encodeMallPurchaseRequest(goodsId, count) {
    const writer = protobuf.Writer.create();
    writer.uint32(8).int64(toLong(goodsId));
    writer.uint32(16).int64(toLong(count));
    return writer.finish();
}

function encodeMallListBySlotTypeRequest(slotType = 1) {
    const writer = protobuf.Writer.create();
    writer.uint32(8).int64(toLong(slotType));
    return writer.finish();
}

function encodeGetRechargeInfoRequest(scene = 'MallUI') {
    const writer = protobuf.Writer.create();
    writer.uint32(10).string(String(scene || 'MallUI')); // field 1
    return writer.finish();
}

async function refreshMallList(slotType = 1) {
    const body = encodeMallListBySlotTypeRequest(slotType);
    await sendMsgAsync('gamepb.mallpb.MallService', 'GetMallListBySlotType', body);
}

async function refreshMallRechargeInfo(scene = 'MallUI') {
    const body = encodeGetRechargeInfoRequest(scene);
    await sendMsgAsync('gamepb.paypb.PayService', 'GetRechargeInfo', body);
}

async function prepareMallContext() {
    await refreshMallList(1);
    await sleep(30);
    await refreshMallRechargeInfo('MallUI');
    await sleep(30);
}

async function mallPurchase(goodsId, count) {
    const body = encodeMallPurchaseRequest(goodsId, count);
    await sendMsgAsync('gamepb.mallpb.MallService', 'Purchase', body);
}

function encodeShareActionRequest(flag = SHARE_DAILY_ACTION_FLAG) {
    const writer = protobuf.Writer.create();
    writer.uint32(8).int64(toLong(flag));
    return writer.finish();
}

async function shareGetInviteInfo() {
    await sendMsgAsync('gamepb.sharepb.ShareService', 'GetInviteInfo', Buffer.alloc(0));
}

async function shareCheckCanShare(flag = SHARE_DAILY_ACTION_FLAG) {
    const body = encodeShareActionRequest(flag);
    await sendMsgAsync('gamepb.sharepb.ShareService', 'CheckCanShare', body);
}

async function shareReportShare(flag = SHARE_DAILY_ACTION_FLAG) {
    const body = encodeShareActionRequest(flag);
    await sendMsgAsync('gamepb.sharepb.ShareService', 'ReportShare', body);
}

async function shareClaimReward(flag = SHARE_DAILY_ACTION_FLAG) {
    const body = encodeShareActionRequest(flag);
    await sendMsgAsync('gamepb.sharepb.ShareService', 'ClaimShareReward', body);
}

function isShareAlreadyDoneError(err) {
    const msg = normalizeUpgradeErrorText(err);
    if (!msg) return false;
    return /已分享|已领取|今日已分享|今日已领取|已经分享|已经领取|already share|already claim|不可分享|不可领取|无可领取|没有可领取/i.test(msg);
}

function encodeItemRef(writer, item) {
    writer.uint32(8).int64(toLong(item.id));
    writer.uint32(16).int64(toLong(item.count));
    if (toNum(item.uid) > 0) {
        writer.uint32(48).int64(toLong(item.uid)); // field 6: uid
    }
}

function encodeBatchUseRequest(items) {
    const writer = protobuf.Writer.create();
    for (const item of items) {
        const one = writer.uint32(10).fork(); // field 1: repeated item
        encodeItemRef(one, item);
        one.ldelim();
    }
    return writer.finish();
}

function encodeUseRequest(item) {
    const writer = protobuf.Writer.create();
    const one = writer.uint32(10).fork(); // field 1
    encodeItemRef(one, item);
    one.ldelim();
    return writer.finish();
}

function encodeCannelNewRequest(itemId) {
    const writer = protobuf.Writer.create();
    writer.uint32(8).int64(toLong(itemId));
    return writer.finish();
}

async function itemBatchUse(items) {
    if (!Array.isArray(items) || items.length === 0) return;
    const body = encodeBatchUseRequest(items);
    await sendMsgAsync('gamepb.itempb.ItemService', 'BatchUse', body);
}

async function itemUse(item) {
    const body = encodeUseRequest(item);
    await sendMsgAsync('gamepb.itempb.ItemService', 'Use', body);
}

async function itemCannelNew(itemId) {
    const body = encodeCannelNewRequest(itemId);
    await sendMsgAsync('gamepb.itempb.ItemService', 'CannelNew', body);
}

async function cancelNewMarksForItems(items) {
    const uniqueNewIds = [...new Set(
        (Array.isArray(items) ? items : [])
            .filter((item) => Boolean(item && (item.isNew || item.is_new)))
            .map((item) => Math.floor(toNum(item.id)))
            .filter((id) => id > 0)
    )];
    for (const itemId of uniqueNewIds) {
        try {
            await itemCannelNew(itemId);
        } catch (e) {
            // ignore
        }
    }
}

function groupNormalBagItems(items) {
    const groups = new Map();
    for (const item of Array.isArray(items) ? items : []) {
        const key = Math.floor(toNum(item.id) / 10); // 8000x/8001x 分组，避免混批触发参数错误
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(item);
    }
    return groups;
}

async function useBundleBagItems(bundleItems, logLabels = {}) {
    let usedKinds = 0;
    let usedCount = 0;
    const bundleErrorLabel = logLabels.bundleErrorLabel || '礼包打开失败';

    for (const item of Array.isArray(bundleItems) ? bundleItems : []) {
        await cancelNewMarksForItems([item]);
        try {
            await itemUse(item);
            usedKinds += 1;
            usedCount += item.count;
        } catch (e) {
            logWarn('道具', `${bundleErrorLabel} id=${item.id} uid=${item.uid}: ${normalizeUpgradeErrorText(e)}`);
        }
        await sleep(30);
    }

    return { usedKinds, usedCount };
}

async function useNormalBagItems(normalItems, logLabels = {}) {
    let usedKinds = 0;
    let usedCount = 0;
    const batchFailLabel = logLabels.batchFailLabel || '批量使用失败，降级单条 BatchUse';
    const singleFailLabel = logLabels.singleFailLabel || '道具使用失败';

    for (const group of groupNormalBagItems(normalItems).values()) {
        for (let i = 0; i < group.length; i += BAG_AUTO_USE_BATCH_SIZE) {
            const chunk = group.slice(i, i + BAG_AUTO_USE_BATCH_SIZE);
            let batchSuccess = false;
            await cancelNewMarksForItems(chunk);
            try {
                await itemBatchUse(chunk);
                batchSuccess = true;
                usedKinds += chunk.length;
                for (const item of chunk) usedCount += item.count;
            } catch (e) {
                logWarn('道具', `${batchFailLabel}: ${normalizeUpgradeErrorText(e)}`);
            }
            if (!batchSuccess) {
                for (const item of chunk) {
                    try {
                        await itemBatchUse([item]);
                        usedKinds += 1;
                        usedCount += item.count;
                    } catch (singleErr) {
                        logWarn('道具', `${singleFailLabel} id=${item.id} uid=${item.uid}: ${normalizeUpgradeErrorText(singleErr)}`);
                    }
                    await sleep(20);
                }
            }
            await sleep(30);
        }
    }

    return { usedKinds, usedCount };
}

function canAutoUseBagItem(item) {
    if (!item) return false;
    const id = toNum(item.id);
    const count = toNum(item.count);
    const uid = toNum(item.uid);
    if (id <= 0 || count <= 0 || uid <= 0) return false;
    if (BAG_AUTO_USE_SKIP_IDS.has(id)) return false;
    if (isBundleItem(id)) return true;
    return id >= BAG_AUTO_USE_DIRECT_ITEM_ID_MIN && id <= BAG_AUTO_USE_DIRECT_ITEM_ID_MAX;
}

function isBundleItem(itemId) {
    const id = toNum(itemId);
    return id >= 100000 && id < 110000; // 如 100003 化肥礼包
}

function getWarehouseBaseItemId(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0) return 0;
    const exact = getItemInfoById(safeId);
    if (exact) return safeId;

    if (safeId >= WAREHOUSE_SUPER_FRUIT_ID_OFFSET) {
        const derived = safeId % WAREHOUSE_SUPER_FRUIT_ID_OFFSET;
        if (derived > 0 && getItemInfoById(derived)) return derived;
    }

    return safeId;
}

function getWarehouseItemInfo(itemId) {
    const baseItemId = getWarehouseBaseItemId(itemId);
    return getItemInfoById(baseItemId);
}

function isWarehouseFruitItemId(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0) return false;

    const itemInfo = getWarehouseItemInfo(safeId);
    const itemType = Math.floor(toNum(itemInfo && itemInfo.type));
    if (itemType === WAREHOUSE_ITEM_TYPE_FRUIT) return true;

    const baseItemId = getWarehouseBaseItemId(safeId);
    if (baseItemId >= 40000 && baseItemId < 50000) return true;
    return !!getPlantByFruitId(baseItemId);
}

function isWarehouseSuperFruitItemId(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0 || safeId < WAREHOUSE_SUPER_FRUIT_ID_OFFSET) return false;
    const baseItemId = getWarehouseBaseItemId(safeId);
    return baseItemId > 0 && baseItemId !== safeId && isWarehouseFruitItemId(baseItemId);
}

function isWarehouseSeedItemId(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0) return false;

    const itemInfo = getWarehouseItemInfo(safeId);
    const itemType = Math.floor(toNum(itemInfo && itemInfo.type));
    const interactionType = String((itemInfo && itemInfo.interaction_type) || '').trim().toLowerCase();
    if (itemType === WAREHOUSE_ITEM_TYPE_SEED || interactionType === 'plant') return true;

    const baseItemId = getWarehouseBaseItemId(safeId);
    if (baseItemId >= 20000 && baseItemId < 30000) return true;
    return isSeedItemId(baseItemId);
}

function classifyWarehouseItemCategory(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0) return '';
    if (isWarehouseSuperFruitItemId(safeId)) return WAREHOUSE_CATEGORY_SUPER_FRUITS;
    if (isWarehouseFruitItemId(safeId)) return WAREHOUSE_CATEGORY_FRUITS;
    if (isWarehouseSeedItemId(safeId)) return WAREHOUSE_CATEGORY_SEEDS;
    return WAREHOUSE_CATEGORY_PROPS;
}

function getWarehouseItemName(itemId) {
    const safeId = Math.floor(toNum(itemId));
    if (safeId <= 0) return '未知物品';

    const exactName = getItemName(safeId);
    if (exactName && exactName !== '未知物品') return exactName;

    const baseItemId = getWarehouseBaseItemId(safeId);
    if (baseItemId > 0 && baseItemId !== safeId) {
        const baseName = getItemName(baseItemId);
        if (baseName && baseName !== '未知物品') return baseName;
    }

    return exactName || `道具${safeId}`;
}

function normalizeWarehouseMutantTypes(item) {
    const rows = Array.isArray(item && item.mutant_types) ? item.mutant_types : [];
    return rows
        .map((value) => Math.floor(toNum(value)))
        .filter((value, index, arr) => value > 0 && arr.indexOf(value) === index)
        .sort((a, b) => a - b);
}

function getWarehouseMutationLabel(mutantTypes, { isSuperFruit = false } = {}) {
    const rows = Array.isArray(mutantTypes) ? mutantTypes : [];
    if (isSuperFruit) return '黄金';
    if (rows.length <= 0) return '普通';

    const labels = rows
        .map((value) => WAREHOUSE_MUTATION_LABELS.get(Math.floor(toNum(value))) || `变异#${Math.floor(toNum(value))}`)
        .filter((value, index, arr) => value && arr.indexOf(value) === index);

    if (labels.length <= 0) return '普通';
    return labels.join('/');
}

function getWarehouseMutantMeta(item, category) {
    const mutantTypes = normalizeWarehouseMutantTypes(item);
    if (category === WAREHOUSE_CATEGORY_SUPER_FRUITS) {
        return {
            mutantTypes,
            mutationLabel: getWarehouseMutationLabel(mutantTypes, { isSuperFruit: true }),
            mutationKey: mutantTypes.length > 0 ? `gold:${mutantTypes.join('-')}` : 'gold',
        };
    }

    if (mutantTypes.length <= 0) {
        return {
            mutantTypes,
            mutationLabel: getWarehouseMutationLabel(mutantTypes),
            mutationKey: 'normal',
        };
    }

    return {
        mutantTypes,
        mutationLabel: getWarehouseMutationLabel(mutantTypes),
        mutationKey: `mutant:${mutantTypes.join('-')}`,
    };
}

function normalizeSeedBaseName(name) {
    return String(name || '').trim().replace(/种子$/u, '').trim();
}

function rememberLargeCropName(name) {
    const baseName = normalizeSeedBaseName(name);
    if (!baseName) return;
    if (KNOWN_LARGE_SEED_NAMES.has(baseName)) {
        runtimeLargeCropNames.add(baseName);
    }
}

function getSeedDisplayName(seedId) {
    const safeSeedId = normalizeSeedId(seedId);
    const plantName = getPlantNameBySeedId(safeSeedId);
    if (plantName && !/^种子\d+$/u.test(String(plantName))) {
        return String(plantName);
    }
    const itemName = normalizeSeedBaseName(getItemName(safeSeedId));
    return itemName || `种子${safeSeedId}`;
}

function getSeedFootprintBySeedId(seedId) {
    const safeSeedId = normalizeSeedId(seedId);
    if (safeSeedId <= 0) return 1;

    const plant = getPlantBySeedId(safeSeedId);
    const plantSize = Math.floor(toNum(plant && plant.size));
    if (plantSize > 1) {
        return plantSize;
    }

    const itemInfo = getItemInfoById(safeSeedId);
    const baseName = normalizeSeedBaseName(
        (itemInfo && itemInfo.effectDesc) || (itemInfo && itemInfo.name) || getItemName(safeSeedId)
    );
    if (KNOWN_LARGE_SEED_IDS.has(safeSeedId) || KNOWN_LARGE_SEED_NAMES.has(baseName) || runtimeLargeCropNames.has(baseName)) {
        return LARGE_CROP_SIZE;
    }

    return 1;
}

function collectOwnedSeedItems(items) {
    const rows = [];
    for (const item of Array.isArray(items) ? items : []) {
        const seedId = normalizeSeedId(item && item.id);
        const count = Math.max(0, Math.floor(toNum(item && item.count)));
        if (seedId <= 0 || count <= 0 || !isSeedItemId(seedId)) continue;
        rows.push({
            seedId,
            count,
            name: getSeedDisplayName(seedId),
            footprint: getSeedFootprintBySeedId(seedId),
        });
    }
    return rows;
}

function makeLargeSeedPlacementKey(seedId, landIds) {
    const safeSeedId = normalizeSeedId(seedId);
    const ids = (Array.isArray(landIds) ? landIds : [])
        .map((id) => Math.floor(toNum(id)))
        .filter((id) => id > 0)
        .sort((a, b) => a - b);
    if (safeSeedId <= 0 || ids.length <= 1) return '';
    return `${safeSeedId}:${ids.join(',')}`;
}

function pruneLargeSeedPlantFailureCache(nowMs = Date.now()) {
    for (const [key, expireAt] of Array.from(largeSeedPlantFailureCache.entries())) {
        if (!Number.isFinite(expireAt) || expireAt <= nowMs) {
            largeSeedPlantFailureCache.delete(key);
        }
    }
}

function isLargeSeedPlacementBlocked(seedId, landIds) {
    pruneLargeSeedPlantFailureCache();
    const key = makeLargeSeedPlacementKey(seedId, landIds);
    if (!key) return false;
    const expireAt = largeSeedPlantFailureCache.get(key);
    return Number.isFinite(expireAt) && expireAt > Date.now();
}

function markLargeSeedPlacementFailure(seedId, landIds, err) {
    const code = extractErrorCode(err);
    const msg = String((err && err.message) || err || '');
    if (code !== 1001052 && !/选择错误的地块格子/u.test(msg)) return false;
    const key = makeLargeSeedPlacementKey(seedId, landIds);
    if (!key) return false;
    largeSeedPlantFailureCache.set(key, Date.now() + LARGE_SEED_PLANT_FAILURE_TTL_MS);
    return true;
}

function applyConsumedSeedCounts(seedItems, placements) {
    const consumed = new Map();
    for (const item of Array.isArray(placements) ? placements : []) {
        if (!item || !item.seedId) continue;
        consumed.set(item.seedId, (consumed.get(item.seedId) || 0) + 1);
    }

    return (Array.isArray(seedItems) ? seedItems : []).map((seed) => ({
        ...seed,
        count: Math.max(0, Math.floor(toNum(seed.count)) - Math.floor(toNum(consumed.get(seed.seedId) || 0))),
    })).filter((seed) => seed.count > 0);
}

function getLargePatchIds(anchorLandId, maxLandId = FARM_UNLOCK_LAND_MAX_ID) {
    const anchor = Math.floor(toNum(anchorLandId));
    if (anchor <= 0 || anchor > maxLandId) return null;
    if (anchor <= LAND_GRID_COLUMNS) return null;
    const col = (anchor - 1) % LAND_GRID_COLUMNS;
    if (col >= LAND_GRID_COLUMNS - 1) return null;
    const patch = [anchor, anchor + 1, anchor - LAND_GRID_COLUMNS, anchor - LAND_GRID_COLUMNS + 1];
    if (patch.some((id) => id <= 0 || id > maxLandId)) return null;
    return patch;
}

function pickLargePatchFromFreeSet(freeLandSet, maxLandId, seedId = 0) {
    for (let anchor = LAND_GRID_COLUMNS + 1; anchor <= maxLandId; anchor += 1) {
        const patch = getLargePatchIds(anchor, maxLandId);
        if (!patch) continue;
        if (isLargeSeedPlacementBlocked(seedId, patch)) continue;
        if (patch.every((id) => freeLandSet.has(id))) {
            return patch;
        }
    }
    return null;
}

function buildBagPlantPlan(seedItems, availableLandIds) {
    const sortedLandIds = Array.from(new Set((Array.isArray(availableLandIds) ? availableLandIds : [])
        .map((id) => Math.floor(toNum(id)))
        .filter((id) => id > 0)))
        .sort((a, b) => a - b);
    const freeLandSet = new Set(sortedLandIds);
    const maxLandId = sortedLandIds.length > 0 ? Math.max(...sortedLandIds, FARM_UNLOCK_LAND_MAX_ID) : FARM_UNLOCK_LAND_MAX_ID;
    const placements = [];

    const largeSeeds = [];
    const singleSeeds = [];
    for (const seed of Array.isArray(seedItems) ? seedItems : []) {
        if (!seed || seed.count <= 0) continue;
        if ((seed.footprint || 1) > 1) largeSeeds.push({ ...seed });
        else singleSeeds.push({ ...seed });
    }

    largeSeeds.sort((a, b) => b.count - a.count || a.seedId - b.seedId);
    for (const seed of largeSeeds) {
        let remain = Math.max(0, Math.floor(toNum(seed.count)));
        while (remain > 0) {
            const patch = pickLargePatchFromFreeSet(freeLandSet, maxLandId, seed.seedId);
            if (!patch) break;
            patch.forEach((id) => freeLandSet.delete(id));
            placements.push({
                seedId: seed.seedId,
                name: seed.name,
                landIds: patch,
                footprint: patch.length,
                source: 'bag',
            });
            remain -= 1;
        }
    }

    const preferredSeedId = getPreferredSeedId();
    singleSeeds.sort((a, b) => {
        if (a.count !== b.count) return a.count - b.count;
        const prefDiff = Number(b.seedId === preferredSeedId) - Number(a.seedId === preferredSeedId);
        if (prefDiff !== 0) return prefDiff;
        return a.seedId - b.seedId;
    });
    for (const seed of singleSeeds) {
        let remain = Math.max(0, Math.floor(toNum(seed.count)));
        while (remain > 0) {
            const nextLandId = Array.from(freeLandSet).sort((a, b) => a - b)[0];
            if (!nextLandId) break;
            freeLandSet.delete(nextLandId);
            placements.push({
                seedId: seed.seedId,
                name: seed.name,
                landIds: [nextLandId],
                footprint: 1,
                source: 'bag',
            });
            remain -= 1;
        }
    }

    return {
        placements,
        remainingLandIds: Array.from(freeLandSet).sort((a, b) => a - b),
    };
}

function summarizeSeedPlacements(placements) {
    const summary = new Map();
    for (const item of Array.isArray(placements) ? placements : []) {
        if (!item || !item.seedId) continue;
        const key = item.seedId;
        const current = summary.get(key) || {
            name: item.name || getSeedDisplayName(item.seedId),
            seedId: item.seedId,
            units: 0,
            cells: 0,
        };
        current.units += 1;
        current.cells += Array.isArray(item.landIds) ? item.landIds.length : 0;
        summary.set(key, current);
    }
    return Array.from(summary.values()).map((item) => {
        if (item.cells > item.units) {
            return `${item.name}x${item.units}(${item.cells}块地)`;
        }
        return `${item.name}x${item.units}`;
    }).join('，');
}

async function executeBagPlantPlan(seedItems, availableLandIds, logPrefix = '优先使用包内种子') {
    const bagPlan = buildBagPlantPlan(seedItems, availableLandIds);
    const placements = bagPlan.placements;
    const successPlacements = [];
    const failedPlacements = [];

    if (placements.length > 0) {
        log('仓库', `${logPrefix}: ${summarizeSeedPlacements(placements)}`);
    }

    for (const placement of placements) {
        const planted = await plantSeeds(placement.seedId, [placement.landIds]);
        if (planted.groups.length > 0) {
            successPlacements.push(...planted.groups.map((group) => ({
                seedId: placement.seedId,
                name: placement.name,
                landIds: group,
                footprint: placement.footprint,
                source: placement.source,
            })));
        }

        if (planted.failures.length > 0) {
            const failure = planted.failures[0];
            failedPlacements.push({
                ...placement,
                error: failure.error,
            });
            if (placement.footprint > 1 && markLargeSeedPlacementFailure(placement.seedId, placement.landIds, failure.error)) {
                logWarn('种植', `${placement.name}(${placement.landIds.join(',')}) 当前不可落在这组地块，已回退其他种子`);
            }
        }
    }

    if (successPlacements.length > 0) {
        log('种植', `已使用包内种子 ${summarizeSeedPlacements(successPlacements)}`);
    }

    const remainingLandIds = Array.from(new Set([
        ...bagPlan.remainingLandIds,
        ...failedPlacements.flatMap((item) => item.landIds || []),
    ])).sort((a, b) => a - b);

    return {
        successPlacements,
        failedPlacements,
        remainingLandIds,
    };
}

function buildLandOccupancyContext(lands) {
    const occupiedSlaveIds = new Set();
    const groupsByMasterId = new Map();

    for (const land of Array.isArray(lands) ? lands : []) {
        if (!land || !land.unlocked) continue;
        const landId = Math.floor(toNum(land.id));
        if (landId <= 0) continue;

        const masterLandId = Math.floor(toNum(land.master_land_id));
        const slaveLandIds = Array.isArray(land.slave_land_ids)
            ? land.slave_land_ids.map((id) => Math.floor(toNum(id))).filter((id) => id > 0 && id !== landId)
            : [];

        if (slaveLandIds.length > 0) {
            groupsByMasterId.set(landId, [landId, ...slaveLandIds]);
            for (const slaveId of slaveLandIds) occupiedSlaveIds.add(slaveId);
            if (land.plant && land.plant.name) {
                rememberLargeCropName(land.plant.name);
            }
            continue;
        }

        if (masterLandId > 0 && masterLandId !== landId) {
            occupiedSlaveIds.add(landId);
            continue;
        }

        groupsByMasterId.set(landId, [landId]);
    }

    return {
        occupiedSlaveIds,
        groupsByMasterId,
    };
}

function shouldHideWarehouseProp(id, uid) {
    const safeId = Math.floor(toNum(id));
    const safeUid = Math.floor(toNum(uid));
    if (safeId <= 0) return true;
    if (WAREHOUSE_HIDDEN_IDS.has(safeId)) return true;
    // 仓库“道具”页仅展示真实道具堆栈（通常都带 uid），过滤各类系统计数器
    if (safeUid <= 0) return true;
    return false;
}

function serializeWarehouseItemForWebui(item) {
    const id = Math.max(0, Math.floor(toNum(item && item.id)));
    const count = Math.max(0, Math.floor(toNum(item && item.count)));
    const uid = Math.max(0, Math.floor(toNum(item && item.uid)));
    if (id <= 0 || count <= 0) return null;

    const category = classifyWarehouseItemCategory(id);
    if (!category) return null;
    if (category === WAREHOUSE_CATEGORY_PROPS && shouldHideWarehouseProp(id, uid)) {
        return null;
    }

    const baseId = getWarehouseBaseItemId(id);
    const itemInfo = getWarehouseItemInfo(id);
    const mutantMeta = getWarehouseMutantMeta(item, category);

    return {
        id,
        baseId,
        uid,
        count,
        isNew: Boolean(item && item.is_new),
        name: getWarehouseItemName(id),
        category,
        itemType: Math.floor(toNum(itemInfo && itemInfo.type)),
        interactionType: String((itemInfo && itemInfo.interaction_type) || '').trim(),
        canUse: Math.floor(toNum(itemInfo && itemInfo.can_use)),
        mutantTypes: mutantMeta.mutantTypes,
        mutationLabel: mutantMeta.mutationLabel,
        mutationKey: mutantMeta.mutationKey,
        isSuperFruit: category === WAREHOUSE_CATEGORY_SUPER_FRUITS,
    };
}

function writeWebuiBagSnapshot(items, reason = 'manual') {
    try {
        const capturedAtMs = Date.now();
        const rows = (Array.isArray(items) ? items : [])
            .map(serializeWarehouseItemForWebui)
            .filter(Boolean);
        const fruits = rows.filter((item) => item.category === WAREHOUSE_CATEGORY_FRUITS);
        const superFruits = rows.filter((item) => item.category === WAREHOUSE_CATEGORY_SUPER_FRUITS);
        const seeds = rows.filter((item) => item.category === WAREHOUSE_CATEGORY_SEEDS);
        const props = rows.filter((item) => item.category === WAREHOUSE_CATEGORY_PROPS);
        const totalCount = rows.reduce((sum, item) => sum + Math.max(0, Number(item.count) || 0), 0);
        const payload = {
            version: 1,
            source: 'qq-farm-bot',
            updated_at: new Date(capturedAtMs).toISOString(),
            captured_at_ms: capturedAtMs,
            reason: String(reason || 'manual'),
            total: rows.length,
            total_count: totalCount,
            fruits,
            superFruits,
            seeds,
            props,
            items: rows, // 向后兼容
        };
        fs.mkdirSync(path.dirname(WEBUI_BAG_ITEMS_PATH), { recursive: true });
        const tmpPath = `${WEBUI_BAG_ITEMS_PATH}.tmp`;
        fs.writeFileSync(tmpPath, `${JSON.stringify(payload)}\n`, 'utf8');
        fs.renameSync(tmpPath, WEBUI_BAG_ITEMS_PATH);
        return {
            totalKinds: rows.length,
            totalCount,
            fruitsKinds: fruits.length,
            superFruitsKinds: superFruits.length,
            seedsKinds: seeds.length,
            propsKinds: props.length,
        };
    } catch (e) {
        logWarn('WEBUI', `写入背包快照失败: ${normalizeUpgradeErrorText(e)}`);
        return null;
    }
}

async function refreshBagSnapshot(reason = 'manual') {
    const items = await fetchBagItems();
    const summary = writeWebuiBagSnapshot(items, reason);
    if (summary) {
        log(
            '仓库',
            `仓库快照已刷新 (${reason})：果实${summary.fruitsKinds}种，超变${summary.superFruitsKinds}种，种子${summary.seedsKinds}种，道具${summary.propsKinds}种`
        );
    }
}

async function claimMallDailyFreeOnce(reason = 'manual') {
    const goodsId = MALL_GOODS_DAILY_FREE_ID;
    await prepareMallContext();
    log('商城', `每日福利领取尝试：goodsId=${goodsId} [每日福利]`);
    try {
        await mallPurchase(goodsId, 1);
    } catch (e) {
        if (extractErrorCode(e) === 1031005) {
            log('商城', '每日福利暂不可领（可能今日已领取）');
            return;
        }
        throw e;
    }
    log('商城', `每日福利领取成功 (${reason}) goodsId=${goodsId} [每日福利]`);
}

async function buyMallNormalFert10h(count, reason = 'manual') {
    const safeCount = Math.max(1, Math.min(MALL_MAX_BUY_10H_COUNT, Math.floor(toNum(count, MALL_DEFAULT_BUY_10H_COUNT))));
    await prepareMallContext();
    const goodsId = MALL_GOODS_FERT_10H_NORMAL_ID;
    log('商城', `10小时化肥购买尝试：goodsId=${goodsId} x${safeCount} [10小时化肥 ${MALL_PRICE_FERT_10H_NORMAL}点券/个]`);
    await mallPurchase(goodsId, safeCount);
    log('商城', `10小时化肥购买成功 (${reason}) goodsId=${goodsId} x${safeCount}`);
}

async function claimDailyShareRewardOnce(reason = 'after-mall-daily') {
    const flag = SHARE_DAILY_ACTION_FLAG;
    log('分享', `每日分享尝试 (${reason})`);

    try {
        await shareGetInviteInfo();
    } catch (e) {
        logWarn('分享', `拉取分享信息失败(继续): ${normalizeUpgradeErrorText(e)}`);
    }

    try {
        await shareCheckCanShare(flag);
    } catch (e) {
        if (!isShareAlreadyDoneError(e)) {
            logWarn('分享', `检查可分享状态失败(继续): ${normalizeUpgradeErrorText(e)}`);
        }
    }

    try {
        await shareReportShare(flag);
    } catch (e) {
        if (isShareAlreadyDoneError(e)) {
            log('分享', '今日已分享，继续尝试领取奖励');
        } else {
            logWarn('分享', `上报分享失败(继续): ${normalizeUpgradeErrorText(e)}`);
        }
    }

    try {
        await shareClaimReward(flag);
        log('分享', `每日分享奖励领取成功 (${reason})`);
        return true;
    } catch (e) {
        if (isShareAlreadyDoneError(e)) {
            log('分享', '每日分享奖励暂不可领（可能今日已领取）');
            return false;
        }
        throw e;
    }
}

async function openAllBagUsableItems(reason = 'manual') {
    let totalUsedKinds = 0;
    let totalUsedCount = 0;
    for (let round = 1; round <= BAG_AUTO_USE_MAX_ROUNDS; round++) {
        const bagItems = await fetchBagItems();
        if (!Array.isArray(bagItems) || bagItems.length === 0) break;

        const candidates = bagItems
            .filter(canAutoUseBagItem)
            .map((item) => ({
                id: toNum(item.id),
                count: Math.max(1, Math.floor(toNum(item.count))),
                uid: toNum(item.uid),
                isNew: Boolean(item.is_new),
            }));
        if (candidates.length === 0) {
            if (round === 1) {
                log('道具', `背包无可自动打开道具 (${reason})`);
            }
            break;
        }

        const bundleItems = candidates.filter((item) => isBundleItem(item.id));
        const normalItems = candidates.filter((item) => !isBundleItem(item.id));

        const normalResult = await useNormalBagItems(normalItems, {
            batchFailLabel: '批量使用失败，降级单条 BatchUse',
            singleFailLabel: '道具使用失败',
        });
        const bundleResult = await useBundleBagItems(bundleItems, {
            bundleErrorLabel: '礼包打开失败',
        });
        const usedThisRound = normalResult.usedKinds + bundleResult.usedKinds;
        totalUsedKinds += normalResult.usedKinds + bundleResult.usedKinds;
        totalUsedCount += normalResult.usedCount + bundleResult.usedCount;

        if (usedThisRound <= 0) break;
        if (round < BAG_AUTO_USE_MAX_ROUNDS) await sleep(80);
    }

    if (totalUsedKinds > 0) {
        log('道具', `打开背包道具完成 (${reason})：处理${totalUsedKinds}种，合计${totalUsedCount}个`);
    }
}

async function openSelectedBagItems(selectedItems, reason = 'manual') {
    const requested = normalizeSelectedBagItems(selectedItems);
    if (requested.length <= 0) {
        log('道具', `勾选道具为空，跳过 (${reason})`);
        return;
    }

    const bagItems = await fetchBagItems();
    if (!Array.isArray(bagItems) || bagItems.length <= 0) {
        log('道具', `背包为空，跳过勾选道具 (${reason})`);
        return;
    }

    const byUid = new Map();
    const byId = new Map();
    for (const raw of bagItems) {
        const id = Math.max(0, Math.floor(toNum(raw && raw.id)));
        const uid = Math.max(0, Math.floor(toNum(raw && raw.uid)));
        const count = Math.max(0, Math.floor(toNum(raw && raw.count)));
        if (id <= 0 || count <= 0) continue;
        const item = { id, uid, count, is_new: Boolean(raw && raw.is_new) };
        if (uid > 0) byUid.set(uid, item);
        if (!byId.has(id)) byId.set(id, []);
        byId.get(id).push(item);
    }

    const candidates = [];
    for (const req of requested) {
        const uid = Math.max(0, Math.floor(toNum(req.uid)));
        const id = Math.max(0, Math.floor(toNum(req.id)));
        let chosen = null;
        if (uid > 0 && byUid.has(uid)) {
            chosen = byUid.get(uid);
        } else if (byId.has(id)) {
            const rows = byId.get(id);
            chosen = rows && rows.length > 0 ? rows[0] : null;
        }
        if (!chosen) continue;
        const useCount = Math.max(1, Math.min(chosen.count, Math.floor(toNum(req.count, chosen.count))));
        candidates.push({
            id: chosen.id,
            uid: chosen.uid,
            count: useCount,
            is_new: chosen.is_new,
            isNew: chosen.is_new,
        });
    }

    if (candidates.length <= 0) {
        log('道具', `勾选道具均不存在于当前背包，跳过 (${reason})`);
        return;
    }

    const bundleItems = candidates.filter((item) => isBundleItem(item.id));
    const normalItems = candidates.filter((item) => !isBundleItem(item.id));

    const normalResult = await useNormalBagItems(normalItems, {
        batchFailLabel: '勾选道具批量失败，降级单条 BatchUse',
        singleFailLabel: '勾选道具使用失败',
    });
    const bundleResult = await useBundleBagItems(bundleItems, {
        bundleErrorLabel: '勾选礼包打开失败',
    });
    const usedKinds = normalResult.usedKinds + bundleResult.usedKinds;
    const usedCount = normalResult.usedCount + bundleResult.usedCount;

    if (usedKinds > 0) {
        log('道具', `打开勾选道具完成 (${reason})：处理${usedKinds}种，合计${usedCount}个`);
    } else {
        log('道具', `打开勾选道具完成 (${reason})：无成功项`);
    }
}

function updateKnownNormalFertilizerSec(value) {
    if (!Number.isFinite(value) || value < 0) return lastKnownNormalFertilizerSec;
    lastKnownNormalFertilizerSec = Math.floor(value);
    lastKnownNormalFertilizerAt = Date.now();
    return lastKnownNormalFertilizerSec;
}

function updateKnownDiamondCount(value) {
    if (!Number.isFinite(value) || value < 0) return lastKnownDiamondCount;
    lastKnownDiamondCount = Math.floor(value);
    lastKnownDiamondAt = Date.now();
    return lastKnownDiamondCount;
}

function getNormalFertilizerCountFromItems(items) {
    for (const item of Array.isArray(items) ? items : []) {
        if (toNum(item && item.id) === NORMAL_FERTILIZER_ID) {
            const count = toNum(item && item.count);
            return Number.isFinite(count) && count >= 0 ? count : 0;
        }
    }
    return 0;
}

function getDiamondCountFromItems(items) {
    for (const item of Array.isArray(items) ? items : []) {
        const id = toNum(item && item.id);
        if (DIAMOND_ITEM_IDS.has(id)) {
            const count = toNum(item && item.count);
            return Number.isFinite(count) && count >= 0 ? count : null;
        }
    }
    return null;
}

function rememberBagDerivedRuntimeStatus(items) {
    const remainSec = getNormalFertilizerCountFromItems(items);
    const diamond = getDiamondCountFromItems(items);
    updateKnownNormalFertilizerSec(remainSec);
    if (Number.isFinite(diamond)) {
        updateKnownDiamondCount(diamond);
    }
    return {
        remainSec,
        diamond: Number.isFinite(diamond) ? diamond : lastKnownDiamondCount,
    };
}

function isNormalFertilizerBagUseCandidate(item) {
    if (!item) return false;
    const id = Math.floor(toNum(item.id));
    const count = Math.max(0, Math.floor(toNum(item.count)));
    const uid = Math.max(0, Math.floor(toNum(item.uid)));
    if (id <= 0 || count <= 0 || uid <= 0) return false;
    const info = getItemInfoById(id);
    const interactionType = String((info && info.interaction_type) || '').trim().toLowerCase();
    return interactionType === 'fertilizer' || id === 100003;
}

function getNormalFertilizerBagUseItemSeconds(itemId) {
    return Math.max(0, Math.floor(toNum(NORMAL_FERTILIZER_BAG_USE_ITEM_SECONDS.get(Math.floor(toNum(itemId))) || 0)));
}

function buildNormalFertilizerUseSelection(items, targetRemainSec = NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC) {
    const currentRemainSec = getNormalFertilizerCountFromItems(items);
    if (!Number.isFinite(currentRemainSec) || currentRemainSec >= targetRemainSec) return [];

    let gapSec = Math.max(0, Math.floor(targetRemainSec - currentRemainSec));
    const candidates = (Array.isArray(items) ? items : [])
        .filter(isNormalFertilizerBagUseCandidate)
        .map((item) => ({
            id: Math.floor(toNum(item.id)),
            uid: Math.max(0, Math.floor(toNum(item.uid))),
            count: Math.max(0, Math.floor(toNum(item.count))),
            isNew: Boolean(item.is_new),
            secondsPerUse: getNormalFertilizerBagUseItemSeconds(item.id),
        }))
        .filter((item) => item.id > 0 && item.uid > 0 && item.count > 0 && item.secondsPerUse > 0)
        .sort((a, b) => b.secondsPerUse - a.secondsPerUse || a.id - b.id || a.uid - b.uid);

    const selected = [];
    for (const item of candidates) {
        const needCount = Math.max(1, Math.ceil(gapSec / item.secondsPerUse));
        const useCount = Math.min(item.count, needCount);
        if (useCount <= 0) continue;
        selected.push({
            id: item.id,
            uid: item.uid,
            count: useCount,
            name: getItemName(item.id),
            isNew: item.isNew,
        });
        gapSec -= useCount * item.secondsPerUse;
        if (gapSec <= 0) break;
    }

    return selected;
}

async function useNormalFertilizerBagItemsToThreshold(targetRemainSec = NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC, reason = 'auto', maxRounds = NORMAL_FERTILIZER_AUTO_USE_MAX_ROUNDS) {
    let touchedBag = false;
    let remainSec = lastKnownNormalFertilizerSec;
    let diamond = lastKnownDiamondCount;

    for (let round = 1; round <= maxRounds; round++) {
        const items = await fetchBagItems();
        const status = rememberBagDerivedRuntimeStatus(items);
        remainSec = status.remainSec;
        diamond = status.diamond;
        const selectedItems = buildNormalFertilizerUseSelection(items, targetRemainSec);
        if (selectedItems.length <= 0) {
            break;
        }
        await openSelectedBagItems(selectedItems, `${reason}#${round}`);
        touchedBag = true;
        await sleep(40);
    }

    if (touchedBag) {
        const items = await fetchBagItems();
        const status = rememberBagDerivedRuntimeStatus(items);
        remainSec = status.remainSec;
        diamond = status.diamond;
    }

    return { touchedBag, remainSec, diamond };
}

function logAutoFertilizeWaitingForNormalFertilizer(reason = 'normal fertilizer low') {
    const now = Date.now();
    if (now - lastNormalFertilizerLowLogAt < NORMAL_FERTILIZER_LOW_LOG_INTERVAL_MS) return false;
    lastNormalFertilizerLowLogAt = now;
    log('施肥', `自动施肥保持开启：当前普通化肥不足，暂时跳过 (${reason})；补充后会自动恢复`);
    return true;
}

async function maybeMaintainNormalFertilizerReserve(reason = 'auto-loop') {
    if (!farmAutoFertilizeEnabled) return false;

    let remainSec = lastKnownNormalFertilizerSec;
    let diamond = lastKnownDiamondCount;
    const now = Date.now();
    const statusStale = !Number.isFinite(remainSec)
        || !Number.isFinite(diamond)
        || (now - Math.max(lastKnownNormalFertilizerAt, lastKnownDiamondAt)) >= NORMAL_FERTILIZER_STATUS_REFRESH_INTERVAL_MS;

    if (statusStale) {
        try {
            const items = await fetchBagItems();
            const status = rememberBagDerivedRuntimeStatus(items);
            remainSec = status.remainSec;
            diamond = status.diamond;
        } catch (e) {
            if (Number.isFinite(remainSec) && remainSec > 0) return true;
            logAutoFertilizeWaitingForNormalFertilizer(`status-refresh-failed ${normalizeUpgradeErrorText(e)}`);
            return false;
        }
    }

    if (Number.isFinite(remainSec) && remainSec < NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC) {
        try {
            const useResult = await useNormalFertilizerBagItemsToThreshold(
                NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC,
                `auto-fertilizer-topup ${reason}`
            );
            remainSec = useResult.remainSec;
            diamond = useResult.diamond;
            if (useResult.touchedBag) {
                try {
                    await refreshBagSnapshot('after-use');
                } catch (e) {
                    logWarn('道具', `使用后刷新背包快照失败: ${normalizeUpgradeErrorText(e)}`);
                }
                await reportFertilizerStatus({ force: true });
                remainSec = lastKnownNormalFertilizerSec;
                diamond = lastKnownDiamondCount;
            }
        } catch (e) {
            logWarn('道具', `普通化肥自动补充失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (
        Number.isFinite(remainSec)
        && remainSec < NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC
        && Number.isFinite(diamond)
        && diamond > MALL_PRICE_FERT_10H_NORMAL
    ) {
        const canTryBuy = (Date.now() - lastNormalFertilizerAutoBuyAttemptAt) >= NORMAL_FERTILIZER_AUTO_BUY_COOLDOWN_MS;
        if (canTryBuy) {
            lastNormalFertilizerAutoBuyAttemptAt = Date.now();
            try {
                await buyMallNormalFert10h(1, `auto-fertilizer-topup ${reason}`);
                await sleep(40);
                const useResult = await useNormalFertilizerBagItemsToThreshold(
                    NORMAL_FERTILIZER_REPLENISH_THRESHOLD_SEC,
                    `after-buy-10h auto ${reason}`
                );
                remainSec = useResult.remainSec;
                diamond = useResult.diamond;
                if (useResult.touchedBag) {
                    try {
                        await refreshBagSnapshot('after-use');
                    } catch (e) {
                        logWarn('道具', `使用后刷新背包快照失败: ${normalizeUpgradeErrorText(e)}`);
                    }
                }
                await reportFertilizerStatus({ force: true });
                remainSec = lastKnownNormalFertilizerSec;
                diamond = lastKnownDiamondCount;
            } catch (e) {
                logWarn('商城', `普通化肥自动购买失败: ${normalizeUpgradeErrorText(e)}`);
            }
        }
    }

    if (Number.isFinite(remainSec) && remainSec <= 0) {
        logAutoFertilizeWaitingForNormalFertilizer(reason);
        return false;
    }

    return !Number.isFinite(remainSec) || remainSec > 0;
}

async function runPendingMallAndItemActions() {
    let touchedBag = false;
    let needFertilizerStatusRefresh = false;

    if (pendingBagSnapshot) {
        const reason = pendingBagSnapshotReason || 'manual';
        pendingBagSnapshot = false;
        pendingBagSnapshotReason = '';
        try {
            await refreshBagSnapshot(reason);
        } catch (e) {
            logWarn('道具', `背包快照刷新失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (pendingMallDailyClaim) {
        const reason = pendingMallDailyClaimReason || 'manual';
        pendingMallDailyClaim = false;
        pendingMallDailyClaimReason = '';
        try {
            await claimMallDailyFreeOnce(reason);
        } catch (e) {
            logWarn('商城', `每日福利领取失败: ${normalizeUpgradeErrorText(e)}`);
        }
        try {
            const shareRewardClaimed = await claimDailyShareRewardOnce(`after-mall-daily ${reason}`);
            if (shareRewardClaimed) {
                touchedBag = true;
            }
        } catch (e) {
            logWarn('分享', `每日分享领取失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (pendingBuy10hFert) {
        const reason = pendingBuy10hFertReason || 'manual';
        const count = pendingBuy10hFertCount;
        pendingBuy10hFert = false;
        pendingBuy10hFertReason = '';
        pendingBuy10hFertCount = MALL_DEFAULT_BUY_10H_COUNT;
        try {
            await buyMallNormalFert10h(count, reason);
            queueUseAllBagItems(`after-buy-10h ${reason}`);
            needFertilizerStatusRefresh = true;
        } catch (e) {
            logWarn('商城', `10小时化肥购买失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (pendingUseSelectedBagItems) {
        const reason = pendingUseSelectedBagItemsReason || 'manual';
        const items = pendingUseSelectedBagItemsItems;
        pendingUseSelectedBagItems = false;
        pendingUseSelectedBagItemsReason = '';
        pendingUseSelectedBagItemsItems = [];
        try {
            await openSelectedBagItems(items, reason);
            touchedBag = true;
        } catch (e) {
            logWarn('道具', `打开勾选道具失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (pendingUseAllBagItems) {
        const reason = pendingUseAllBagItemsReason || 'manual';
        pendingUseAllBagItems = false;
        pendingUseAllBagItemsReason = '';
        try {
            await openAllBagUsableItems(reason);
            touchedBag = true;
        } catch (e) {
            logWarn('道具', `打开背包道具失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }

    if (touchedBag) {
        try {
            await refreshBagSnapshot('after-use');
        } catch (e) {
            logWarn('道具', `使用后刷新背包快照失败: ${normalizeUpgradeErrorText(e)}`);
        }
        needFertilizerStatusRefresh = true;
    }

    if (needFertilizerStatusRefresh) {
        try {
            await reportFertilizerStatus({ force: true });
        } catch (e) {
            logWarn('施肥', `刷新化肥状态失败: ${normalizeUpgradeErrorText(e)}`);
        }
    }
}

async function harvest(landIds) {
    const state = getUserState();
    const body = types.HarvestRequest.encode(types.HarvestRequest.create({
        land_ids: landIds,
        host_gid: toLong(state.gid),
        is_all: true,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Harvest', body);
    return types.HarvestReply.decode(replyBody);
}

async function waterLand(landIds) {
    const state = getUserState();
    const body = types.WaterLandRequest.encode(types.WaterLandRequest.create({
        land_ids: landIds,
        host_gid: toLong(state.gid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'WaterLand', body);
    return types.WaterLandReply.decode(replyBody);
}

async function weedOut(landIds) {
    const state = getUserState();
    const body = types.WeedOutRequest.encode(types.WeedOutRequest.create({
        land_ids: landIds,
        host_gid: toLong(state.gid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'WeedOut', body);
    return types.WeedOutReply.decode(replyBody);
}

async function insecticide(landIds) {
    const state = getUserState();
    const body = types.InsecticideRequest.encode(types.InsecticideRequest.create({
        land_ids: landIds,
        host_gid: toLong(state.gid),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Insecticide', body);
    return types.InsecticideReply.decode(replyBody);
}

// 普通肥料 ID
const NORMAL_FERTILIZER_ID = 1011;

async function getNormalFertilizerCount() {
    try {
        const items = await fetchBagItems();
        const status = rememberBagDerivedRuntimeStatus(items);
        return status.remainSec;
    } catch (e) {
        return null;
    }
}

/**
 * 施肥 - 必须逐块进行，服务器不支持批量
 * 游戏中拖动施肥间隔很短，这里用 50ms
 */
async function fertilize(landIds, fertilizerId = NORMAL_FERTILIZER_ID) {
    if (fertilizerId === NORMAL_FERTILIZER_ID) {
        if (!farmAutoFertilizeEnabled) {
            return 0;
        }
        const canAutoFertilize = await maybeMaintainNormalFertilizerReserve('before-fertilize');
        if (!canAutoFertilize) {
            return 0;
        }
    }

    let successCount = 0;
    let lastFertilizerRemainSec = null;
    for (const landId of landIds) {
        try {
            const body = types.FertilizeRequest.encode(types.FertilizeRequest.create({
                land_ids: [toLong(landId)],
                fertilizer_id: toLong(fertilizerId),
            })).finish();
            const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Fertilize', body);
            if (fertilizerId === NORMAL_FERTILIZER_ID && replyBody) {
                try {
                    const reply = types.FertilizeReply.decode(replyBody);
                    const remainSec = toNum(reply.fertilizer);
                    if (Number.isFinite(remainSec) && remainSec >= 0) {
                        lastFertilizerRemainSec = remainSec;
                        updateKnownNormalFertilizerSec(remainSec);
                    }
                } catch (e) {
                    // 忽略解码失败，后续走 Bag 查询兜底
                }
            }
            successCount++;
        } catch (e) {
            // 施肥失败（可能肥料不足），停止继续
            break;
        }
        if (landIds.length > 1) await sleep(50);  // 50ms 间隔
    }

    if (fertilizerId === NORMAL_FERTILIZER_ID && landIds.length > 0) {
        if (Number.isFinite(lastFertilizerRemainSec)) {
            updateKnownNormalFertilizerSec(lastFertilizerRemainSec);
            if (lastFertilizerRemainSec > 0) {
                log('施肥', `FERT_STATUS normal_sec=${lastFertilizerRemainSec}`);
            } else {
                log('施肥', 'FERT_STATUS low normal_sec=0');
                logAutoFertilizeWaitingForNormalFertilizer('normal fertilizer low normal_sec=0');
            }
            lastFertStatusLogAt = Date.now();
        } else {
            const remaining = await getNormalFertilizerCount();
            if (Number.isFinite(remaining)) {
                updateKnownNormalFertilizerSec(remaining);
                if (remaining > 0) {
                    log('施肥', `FERT_STATUS normal_count=${remaining}`);
                } else {
                    log('施肥', 'FERT_STATUS low normal_count=0');
                    logAutoFertilizeWaitingForNormalFertilizer('normal fertilizer low normal_count=0');
                }
                lastFertStatusLogAt = Date.now();
            } else if (successCount >= landIds.length) {
                log('施肥', 'FERT_STATUS ok');
            } else {
                log('施肥', 'FERT_STATUS low');
            }
        }
    }

    return successCount;
}

async function reportFertilizerStatus(options = {}) {
    const { force = false } = options || {};
    if (!force && Date.now() - lastFertStatusLogAt < FERT_STATUS_CHECK_INTERVAL_MS) return;
    try {
        const items = await fetchBagItems();
        const status = rememberBagDerivedRuntimeStatus(items);
        if (Number.isFinite(status.remainSec)) {
            if (status.remainSec > 0) {
                log('施肥', `FERT_STATUS normal_count=${status.remainSec}`);
            } else {
                log('施肥', 'FERT_STATUS low normal_count=0');
            }
            lastFertStatusLogAt = Date.now();
        }
    } catch (e) {
        // 查询失败不影响主流程
    }
}

/**
 * 主动查询普通化肥余量并上报给 webui（无论是否正在施肥）
 * 仅当距上次上报超过 FERT_STATUS_CHECK_INTERVAL_MS 时才执行
 */
async function reportFertilizerStatusIfStale() {
    await reportFertilizerStatus({ force: false });
}

async function removePlant(landIds) {
    const body = types.RemovePlantRequest.encode(types.RemovePlantRequest.create({
        land_ids: landIds.map(id => toLong(id)),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'RemovePlant', body);
    return types.RemovePlantReply.decode(replyBody);
}

// ============ 商店 API ============

async function getShopInfo(shopId) {
    const body = types.ShopInfoRequest.encode(types.ShopInfoRequest.create({
        shop_id: toLong(shopId),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.shoppb.ShopService', 'ShopInfo', body);
    return types.ShopInfoReply.decode(replyBody);
}

async function buyGoods(goodsId, num, price) {
    const body = types.BuyGoodsRequest.encode(types.BuyGoodsRequest.create({
        goods_id: toLong(goodsId),
        num: toLong(num),
        price: toLong(price),
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.shoppb.ShopService', 'BuyGoods', body);
    return types.BuyGoodsReply.decode(replyBody);
}

// ============ 种植 ============

function encodePlantRequest(seedId, landIds) {
    const writer = protobuf.Writer.create();
    const itemWriter = writer.uint32(18).fork();
    itemWriter.uint32(8).int64(seedId);
    const idsWriter = itemWriter.uint32(18).fork();
    for (const id of landIds) {
        idsWriter.int64(id);
    }
    idsWriter.ldelim();
    itemWriter.ldelim();
    return writer.finish();
}

/**
 * 种植 - 游戏中拖动种植间隔很短，这里用 50ms
 */
async function plantSeeds(seedId, landTargets) {
    const targets = (Array.isArray(landTargets) ? landTargets : [])
        .map((target) => Array.isArray(target) ? target : [target])
        .map((group) => group
            .map((id) => Math.floor(toNum(id)))
            .filter((id) => id > 0))
        .filter((group) => group.length > 0);
    let successCount = 0;
    const successGroups = [];
    const failures = [];
    for (const landIds of targets) {
        try {
            const body = encodePlantRequest(seedId, landIds);
            const { body: replyBody } = await sendMsgAsync('gamepb.plantpb.PlantService', 'Plant', body);
            types.PlantReply.decode(replyBody);
            successCount += landIds.length;
            successGroups.push(landIds);
        } catch (e) {
            logWarn('种植', `土地#${landIds.join(',')} 失败: ${e.message}`);
            failures.push({
                landIds,
                error: e,
            });
        }
        if (targets.length > 1) await sleep(50);  // 50ms 间隔
    }
    return {
        cells: successCount,
        groups: successGroups,
        failures,
    };
}

function normalizeSeedId(value) {
    const n = Number(value);
    return Number.isInteger(n) && n >= 0 ? n : 0;
}

function normalizeIntervalSec(value, fallbackSec) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallbackSec;
    return Math.min(Math.max(Math.floor(n), 0), 300);
}

function readWebuiConfig() {
    try {
        const stat = fs.statSync(WEBUI_CONFIG_PATH);
        if (stat.mtimeMs === webuiConfigMtimeMs) {
            return webuiConfigCache;
        }

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

function getPreferredSeedId() {
    const cfg = readWebuiConfig();
    if (cfg && Object.prototype.hasOwnProperty.call(cfg, 'preferredSeedId')) {
        return normalizeSeedId(cfg.preferredSeedId);
    }
    return normalizeSeedId(CONFIG.targetSeedId);
}

function writeWebuiConfigPatch(patch = {}) {
    try {
        const current = readWebuiConfig() || {};
        const next = { ...current, ...patch };
        fs.mkdirSync(path.dirname(WEBUI_CONFIG_PATH), { recursive: true });
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
        logWarn('CONFIG', `write webui config failed: ${sanitizeLogText(err && err.message ? err.message : String(err))}`);
        return null;
    }
}

function disableAutoFertilizeDueToLow(reason = 'normal fertilizer low') {
    return logAutoFertilizeWaitingForNormalFertilizer(reason);
}

function getLatestUnlockedLandCountForRecommendation() {
    try {
        const payload = JSON.parse(fs.readFileSync(WEBUI_LANDS_PATH, 'utf8')) || {};
        const unlockedCount = toNum(payload.unlocked_count);
        if (unlockedCount > 0) return unlockedCount;
        const total = toNum(payload.total);
        if (total > 0) return total;
    } catch (e) {
        // ignore snapshot read failure and use fallback
    }
    return LEVEL_UP_RECOMMEND_FALLBACK_LANDS;
}

function applyPreferredSeedTargetByNormalFert(level, landsCount = 0, reason = 'runtime') {
    const safeLevel = Math.max(1, Math.floor(toNum(level, 1)));
    const landsForCalc = landsCount > 0 ? Math.floor(toNum(landsCount, 0)) : getLatestUnlockedLandCountForRecommendation();
    let recommendation;
    try {
        recommendation = getPlantingRecommendation(safeLevel, landsForCalc, { top: 20 });
    } catch (e) {
        logWarn('CONFIG', `preferred seed calc failed (${reason}): ${sanitizeLogText(e && e.message ? e.message : String(e))}`);
        return;
    }

    const bestNormalFert = recommendation && recommendation.bestNormalFert ? recommendation.bestNormalFert : null;
    const nextSeedId = normalizeSeedId(bestNormalFert && bestNormalFert.seedId);
    if (nextSeedId <= 0) {
        logWarn('CONFIG', `preferred seed skipped (${reason}): no bestNormalFert`);
        return;
    }

    const currentCfg = readWebuiConfig() || {};
    const currentPreferredSeedId = normalizeSeedId(currentCfg.preferredSeedId);
    if (currentPreferredSeedId === nextSeedId) {
        log('CONFIG', `preferred seed keep ${bestNormalFert.name}(${nextSeedId}) (Lv${safeLevel}, lands=${landsForCalc}, reason=${reason})`);
        return;
    }

    const updated = writeWebuiConfigPatch({ preferredSeedId: nextSeedId });
    if (!updated) return;
    log('CONFIG', `preferred seed set to ${bestNormalFert.name}(${nextSeedId}) (Lv${safeLevel}, lands=${landsForCalc}, reason=${reason})`);
}

function applyLevelUpPreferredSeedTarget(newLevel, oldLevel = 0) {
    const safeLevel = Math.max(1, Math.floor(toNum(newLevel, 1)));
    applyPreferredSeedTargetByNormalFert(
        safeLevel,
        0,
        `level-up Lv${oldLevel}->Lv${safeLevel}`
    );
}

function scheduleFarmCheckSoon(tag = 'runtime') {
    if (farmLoopRunning && !isCheckingFarm) {
        setTimeout(() => {
            if (!farmLoopRunning || isCheckingFarm) return;
            checkFarm().catch((err) => {
                logWarn(tag, `触发巡田失败: ${err && err.message ? err.message : String(err)}`);
            });
        }, 120);
    }
}

function queueLandUpgradeSweep(reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const shouldLog = !pendingLandUpgradeSweep;
    pendingLandUpgradeSweep = true;
    pendingLandUpgradeSweepReason = normalizedReason;

    if (shouldLog) {
        log('升级土地', `已排队: ${normalizedReason}`);
    }

    scheduleFarmCheckSoon('升级土地');
}

function queueMallDailyClaim(reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const shouldLog = !pendingMallDailyClaim;
    pendingMallDailyClaim = true;
    pendingMallDailyClaimReason = normalizedReason;
    if (shouldLog) {
        log('商城', `每日福利领取已排队: ${normalizedReason}`);
    }
    scheduleFarmCheckSoon('商城');
}

function queueBuy10hFert(count, reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const normalizedCount = Math.max(1, Math.min(MALL_MAX_BUY_10H_COUNT, Math.floor(toNum(count, MALL_DEFAULT_BUY_10H_COUNT))));
    const shouldLog = !pendingBuy10hFert;
    pendingBuy10hFert = true;
    pendingBuy10hFertReason = normalizedReason;
    pendingBuy10hFertCount = normalizedCount;
    if (shouldLog) {
        log('商城', `10小时化肥购买已排队: x${normalizedCount} (${normalizedReason})`);
    }
    scheduleFarmCheckSoon('商城');
}

function queueUseAllBagItems(reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const shouldLog = !pendingUseAllBagItems;
    pendingUseAllBagItems = true;
    pendingUseAllBagItemsReason = normalizedReason;
    if (shouldLog) {
        log('道具', `打开全部道具已排队: ${normalizedReason}`);
    }
    scheduleFarmCheckSoon('道具');
}

function normalizeSelectedBagItems(items) {
    if (!Array.isArray(items)) return [];
    const normalized = [];
    for (const one of items) {
        if (!one || typeof one !== 'object') continue;
        if (normalized.length >= 500) break;
        const id = Math.floor(toNum(one.id));
        if (id <= 0) continue;
        const uid = Math.max(0, Math.floor(toNum(one.uid)));
        const count = Math.max(1, Math.floor(toNum(one.count, 1)));
        const name = String(one.name || '').trim().slice(0, 80);
        normalized.push({ id, uid, count, name });
    }
    return normalized;
}

function queueBagSnapshot(reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const shouldLog = !pendingBagSnapshot;
    pendingBagSnapshot = true;
    pendingBagSnapshotReason = normalizedReason;
    if (shouldLog) {
        log('道具', `背包快照刷新已排队: ${normalizedReason}`);
    }
    scheduleFarmCheckSoon('道具');
}

function queueUseSelectedBagItems(items, reason = 'manual') {
    const normalizedReason = String(reason || 'manual').trim() || 'manual';
    const normalizedItems = normalizeSelectedBagItems(items);
    if (normalizedItems.length <= 0) {
        logWarn('道具', `勾选道具请求为空，忽略 (${normalizedReason})`);
        return;
    }
    pendingUseSelectedBagItems = true;
    pendingUseSelectedBagItemsReason = normalizedReason;
    pendingUseSelectedBagItemsItems = normalizedItems;
    log('道具', `勾选道具已排队: ${normalizedItems.length}项 (${normalizedReason})`);
    scheduleFarmCheckSoon('道具');
}

function getLocalDateKey(date = new Date()) {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

function maybeQueueDailyMallClaim() {
    const now = new Date();
    const dateKey = getLocalDateKey(now);
    const hh = now.getHours();
    const mm = now.getMinutes();
    const reached = hh > MALL_DAILY_AUTO_HOUR || (hh === MALL_DAILY_AUTO_HOUR && mm >= MALL_DAILY_AUTO_MINUTE);
    if (!reached) return;
    if (lastMallDailyAutoClaimDateKey === dateKey) return;
    lastMallDailyAutoClaimDateKey = dateKey;
    queueMallDailyClaim('daily-00:05');
}


function clearFastHarvestTimer(landId) {
    const id = Math.max(0, Math.floor(toNum(landId)));
    if (id <= 0) return false;
    const entry = fastHarvestTimers.get(id);
    if (!entry) return false;
    try {
        clearTimeout(entry.timer);
    } catch (e) {
        // ignore timer cleanup failure
    }
    fastHarvestTimers.delete(id);
    return true;
}

function clearAllFastHarvestTimers() {
    for (const landId of Array.from(fastHarvestTimers.keys())) {
        clearFastHarvestTimer(landId);
    }
}

function syncFastHarvestTimers(soonToMature = []) {
    const nextByLandId = new Map();
    for (const item of Array.isArray(soonToMature) ? soonToMature : []) {
        const landId = Math.max(0, Math.floor(toNum(item && item.landId)));
        const matureTime = Math.max(0, Math.floor(toNum(item && item.matureTime)));
        if (landId <= 0 || matureTime <= 0) continue;
        nextByLandId.set(landId, {
            landId,
            matureTime,
            plantName: String(item && item.plantName || '').trim(),
        });
    }

    for (const [landId, entry] of Array.from(fastHarvestTimers.entries())) {
        const next = nextByLandId.get(landId);
        if (!farmFastHarvestEnabled || !next || next.matureTime !== entry.matureTime) {
            clearFastHarvestTimer(landId);
        }
    }

    if (!farmFastHarvestEnabled) return;

    const nowSec = getServerTimeSec();
    for (const item of nextByLandId.values()) {
        if (fastHarvestTimers.has(item.landId)) continue;
        const waitSec = item.matureTime - nowSec;
        if (waitSec <= 0 || waitSec > FAST_HARVEST_PREPARE_WINDOW_SEC) continue;

        const waitMs = Math.max(0, (waitSec * 1000) - FAST_HARVEST_EARLY_TRIGGER_MS);
        const timer = setTimeout(async () => {
            fastHarvestTimers.delete(item.landId);
            if (!farmLoopRunning) return;

            try {
                await harvest([item.landId]);
                scheduleIllustratedClaim(600);
                log('秒收', `${item.plantName || `地块#${item.landId}`} 已触发秒收`);
                scheduleFarmCheckSoon('秒收');
            } catch (e) {
                const msg = sanitizeLogText(e && e.message ? e.message : String(e));
                logWarn('秒收', `${item.plantName || `地块#${item.landId}`} 秒收失败: ${msg}`);
            }
        }, waitMs);

        fastHarvestTimers.set(item.landId, {
            timer,
            matureTime: item.matureTime,
            plantName: item.plantName,
        });
        log('秒收', `已预设 ${item.plantName || `地块#${item.landId}`}，距离成熟 ${waitSec}s`);
    }
}
function shouldIgnoreStaleRequest(reqAtMs) {
    const n = Number(reqAtMs || 0);
    if (!Number.isFinite(n) || n <= 0) return false;
    return (Date.now() - n) > LAND_UPGRADE_MANUAL_REQUEST_MAX_AGE_MS;
}

function applyRuntimeFarmConfig() {
    const cfg = readWebuiConfig();
    if (!cfg) return;

    if (Object.prototype.hasOwnProperty.call(cfg, 'intervalSec')) {
        const currentSec = Math.max(0, Math.floor(CONFIG.farmCheckInterval / 1000));
        const nextSec = normalizeIntervalSec(cfg.intervalSec, currentSec);
        const nextMs = nextSec * 1000;
        if (nextMs !== CONFIG.farmCheckInterval) {
            CONFIG.farmCheckInterval = nextMs;
            log('CONFIG', `farm interval hot-updated to ${nextSec}s`);
        }
    }

    const nextFastHarvest = cfg.fastHarvest === undefined ? true : Boolean(cfg.fastHarvest);
    if (nextFastHarvest !== farmFastHarvestEnabled) {
        farmFastHarvestEnabled = nextFastHarvest;
        if (!farmFastHarvestEnabled) {
            clearAllFastHarvestTimers();
        }
        log('CONFIG', `fast harvest hot-updated to ${farmFastHarvestEnabled ? 'on' : 'off'}`);
    }

    const nextAutoFertilize = cfg.autoFertilize === undefined ? true : Boolean(cfg.autoFertilize);
    if (nextAutoFertilize !== farmAutoFertilizeEnabled) {
        farmAutoFertilizeEnabled = nextAutoFertilize;
        log('CONFIG', `auto fertilize hot-updated to ${farmAutoFertilizeEnabled ? 'on' : 'off'}`);
    }

    const reqIdRaw = cfg.landUpgradeSweepRequestId;
    const reqId = reqIdRaw === undefined || reqIdRaw === null ? '' : String(reqIdRaw).trim();
    if (reqId && reqId !== lastHandledLandUpgradeSweepRequestId) {
        lastHandledLandUpgradeSweepRequestId = reqId;
        const reqAtMs = Number(cfg.landUpgradeSweepRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale land upgrade sweep request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            queueLandUpgradeSweep('manual-button');
        }
    }

    const mallDailyReqIdRaw = cfg.mallDailyClaimRequestId;
    const mallDailyReqId = mallDailyReqIdRaw === undefined || mallDailyReqIdRaw === null ? '' : String(mallDailyReqIdRaw).trim();
    if (mallDailyReqId && mallDailyReqId !== lastHandledMallDailyClaimRequestId) {
        lastHandledMallDailyClaimRequestId = mallDailyReqId;
        const reqAtMs = Number(cfg.mallDailyClaimRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale mall daily request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            queueMallDailyClaim('manual-button');
        }
    }

    const buy10hReqIdRaw = cfg.mallBuy10hFertRequestId;
    const buy10hReqId = buy10hReqIdRaw === undefined || buy10hReqIdRaw === null ? '' : String(buy10hReqIdRaw).trim();
    if (buy10hReqId && buy10hReqId !== lastHandledBuy10hFertRequestId) {
        lastHandledBuy10hFertRequestId = buy10hReqId;
        const reqAtMs = Number(cfg.mallBuy10hFertRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale buy-10h-fert request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            const count = Math.max(1, Math.min(
                MALL_MAX_BUY_10H_COUNT,
                Math.floor(toNum(cfg.mallBuy10hFertCount, MALL_DEFAULT_BUY_10H_COUNT))
            ));
            queueBuy10hFert(count, 'manual-button');
        }
    }

    const useAllReqIdRaw = cfg.bagUseAllRequestId;
    const useAllReqId = useAllReqIdRaw === undefined || useAllReqIdRaw === null ? '' : String(useAllReqIdRaw).trim();
    if (useAllReqId && useAllReqId !== lastHandledUseAllBagItemsRequestId) {
        lastHandledUseAllBagItemsRequestId = useAllReqId;
        const reqAtMs = Number(cfg.bagUseAllRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale bag-use-all request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            queueUseAllBagItems('manual-button');
        }
    }

    const bagSnapshotReqIdRaw = cfg.bagSnapshotRequestId;
    const bagSnapshotReqId = bagSnapshotReqIdRaw === undefined || bagSnapshotReqIdRaw === null ? '' : String(bagSnapshotReqIdRaw).trim();
    if (bagSnapshotReqId && bagSnapshotReqId !== lastHandledBagSnapshotRequestId) {
        lastHandledBagSnapshotRequestId = bagSnapshotReqId;
        const reqAtMs = Number(cfg.bagSnapshotRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale bag-snapshot request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            queueBagSnapshot('manual-button');
        }
    }

    const useSelectedReqIdRaw = cfg.bagUseSelectedRequestId;
    const useSelectedReqId = useSelectedReqIdRaw === undefined || useSelectedReqIdRaw === null ? '' : String(useSelectedReqIdRaw).trim();
    if (useSelectedReqId && useSelectedReqId !== lastHandledUseSelectedBagItemsRequestId) {
        lastHandledUseSelectedBagItemsRequestId = useSelectedReqId;
        const reqAtMs = Number(cfg.bagUseSelectedRequestedAtMs || 0);
        if (shouldIgnoreStaleRequest(reqAtMs)) {
            const ageMs = Date.now() - reqAtMs;
            log('CONFIG', `ignore stale bag-use-selected request (${Math.floor(ageMs / 1000)}s old)`);
        } else {
            queueUseSelectedBagItems(cfg.bagUseSelectedItems, 'manual-button');
        }
    }
}

function extractErrorCode(err) {
    const msg = String((err && err.message) || err || '');
    const match = msg.match(/code=(\d+)/);
    return match ? Number(match[1]) : 0;
}

function getLandUpgradeErrorLabel(err) {
    const msg = String((err && err.message) || err || '');
    if (/解锁未生效/.test(msg)) return '解锁未生效';
    if (/土地未解锁/.test(msg)) return '土地未解锁';
    if (/种植等级不足/.test(msg)) return '种植等级不足';
    if (/金币不足/.test(msg)) return '金币不足';
    const code = extractErrorCode(err);
    if (code > 0 && LAND_UPGRADE_ERROR_LABELS.has(code)) {
        return LAND_UPGRADE_ERROR_LABELS.get(code);
    }
    return '';
}

function shouldIgnoreActionError(actionTag, err) {
    const code = extractErrorCode(err);
    const codes = BENIGN_ACTION_ERROR_CODES[actionTag];
    return Boolean(codes && codes.has(code));
}

function logActionError(actionTag, err) {
    if (shouldIgnoreActionError(actionTag, err)) return;
    logWarn(actionTag, err.message || String(err));
}

async function findBestSeed(landsCount, options = {}) {
    const SEED_SHOP_ID = 2;
    const maxFootprint = Math.max(1, Math.floor(toNum(options && options.maxFootprint, 1)));
    const shopReply = await getShopInfo(SEED_SHOP_ID);
    if (!shopReply.goods_list || shopReply.goods_list.length === 0) {
        logWarn('商店', '种子商店无商品');
        return null;
    }

    const state = getUserState();
    const available = [];
    for (const goods of shopReply.goods_list) {
        if (!goods.unlocked) continue;

        let meetsConditions = true;
        let requiredLevel = 0;
        const conds = goods.conds || [];
        for (const cond of conds) {
            if (toNum(cond.type) === 1) {
                requiredLevel = toNum(cond.param);
                if (state.level < requiredLevel) {
                    meetsConditions = false;
                    break;
                }
            }
        }
        if (!meetsConditions) continue;

        const limitCount = toNum(goods.limit_count);
        const boughtNum = toNum(goods.bought_num);
        if (limitCount > 0 && boughtNum >= limitCount) continue;

        const seedId = toNum(goods.item_id);
        const footprint = getSeedFootprintBySeedId(seedId);
        if (footprint > maxFootprint) continue;

        available.push({
            goods,
            goodsId: toNum(goods.id),
            seedId,
            price: toNum(goods.price),
            requiredLevel,
            footprint,
        });
    }

    if (available.length === 0) {
        logWarn('商店', '没有可购买的种子');
        return null;
    }

    // 默认按最低等级(大萝卜)优先，支持配置指定作物。
    available.sort((a, b) => {
        if (a.requiredLevel !== b.requiredLevel) return a.requiredLevel - b.requiredLevel;
        if (a.price !== b.price) return a.price - b.price;
        return a.seedId - b.seedId;
    });

    const preferredSeedId = getPreferredSeedId();
    if (preferredSeedId > 0) {
        const matched = available.find((item) => item.seedId === preferredSeedId);
        if (matched) {
            lastSeedFallbackKey = '';
            return matched;
        }

        const fallbackKey = `${preferredSeedId}:${available.map((item) => item.seedId).join(',')}`;
        if (fallbackKey !== lastSeedFallbackKey) {
            const preferredName = getPlantNameBySeedId(preferredSeedId);
            logWarn('商店', `指定作物 ${preferredName}(${preferredSeedId}) 当前不可购买，已回退默认作物`);
            lastSeedFallbackKey = fallbackKey;
        }
    } else {
        lastSeedFallbackKey = '';
    }

    if (CONFIG.forceLowestLevelCrop) {
        return available[0];
    }

    try {
        log('商店', `等级: ${state.level}，土地数量: ${landsCount}`);
        const rec = getPlantingRecommendation(state.level, landsCount == null ? 18 : landsCount, { top: 50 });
        const rankedSeedIds = rec.candidatesNormalFert.map(x => x.seedId);
        for (const seedId of rankedSeedIds) {
            const hit = available.find(x => x.seedId === seedId);
            if (hit) return hit;
        }
    } catch (e) {
        logWarn('商店', `经验效率推荐失败，使用兜底策略: ${e.message}`);
    }

    // 兜底：等级在28级以前更偏向低级作物，28级以上偏向高级作物
    if (state.level && state.level <= 28) {
        available.sort((a, b) => a.requiredLevel - b.requiredLevel || a.price - b.price);
    } else {
        available.sort((a, b) => b.requiredLevel - a.requiredLevel || a.price - b.price);
    }

    return available[0];
}

async function autoPlantEmptyLands(deadLandIds, emptyLandIds, unlockedLandCount) {
    let landsToPlant = Array.from(new Set((Array.isArray(emptyLandIds) ? emptyLandIds : [])
        .map((id) => Math.floor(toNum(id)))
        .filter((id) => id > 0)))
        .sort((a, b) => a - b);
    const state = getUserState();

    // 1. 铲除枯死/收获残留植物（一键操作）
    if (deadLandIds.length > 0) {
        try {
            await removePlant(deadLandIds);
            log('铲除', `已铲除 ${deadLandIds.length} 块 (${deadLandIds.join(',')})`);
            const refreshed = await getAllLands();
            const refreshedLands = Array.isArray(refreshed && refreshed.lands) ? refreshed.lands : [];
            writeWebuiLandsSnapshot(refreshedLands);
            const refreshedStatus = analyzeLands(refreshedLands);
            landsToPlant = Array.from(new Set([
                ...landsToPlant,
                ...refreshedStatus.empty,
            ])).sort((a, b) => a - b);
        } catch (e) {
            logWarn('铲除', `批量铲除失败: ${e.message}`);
            // 失败时仍然尝试种植
            landsToPlant = Array.from(new Set([...landsToPlant, ...deadLandIds])).sort((a, b) => a - b);
        }
    }

    if (landsToPlant.length === 0) return;

    let bagItems = [];
    try {
        bagItems = await fetchBagItems();
    } catch (e) {
        logWarn('仓库', `查询包内种子失败: ${e.message}`);
    }

    const ownedSeeds = collectOwnedSeedItems(bagItems);
    const fertilizeTargets = [];
    let remainingLandIds = landsToPlant;

    if (ownedSeeds.length > 0) {
        const bagAttempt = await executeBagPlantPlan(ownedSeeds, remainingLandIds, '优先使用包内种子');
        fertilizeTargets.push(...bagAttempt.successPlacements.map((item) => item.landIds[0]));
        remainingLandIds = bagAttempt.remainingLandIds;

        const remainingOwnedSeeds = applyConsumedSeedCounts(ownedSeeds, bagAttempt.successPlacements);
        if (remainingLandIds.length > 0 && remainingOwnedSeeds.length > 0) {
            const retryAttempt = await executeBagPlantPlan(remainingOwnedSeeds, remainingLandIds, '回退使用包内种子');
            fertilizeTargets.push(...retryAttempt.successPlacements.map((item) => item.landIds[0]));
            remainingLandIds = retryAttempt.remainingLandIds;
        }
    }

    if (remainingLandIds.length === 0) {
        const uniqueFertilizeTargets = Array.from(new Set(fertilizeTargets));
        if (uniqueFertilizeTargets.length > 0) {
            const fertilized = await fertilize(uniqueFertilizeTargets);
            if (fertilized > 0) {
                log('施肥', `已为 ${fertilized}/${uniqueFertilizeTargets.length} 块地施肥`);
            }
        }
        return;
    }

    // 2. 查询种子商店
    let bestSeed;
    try {
        bestSeed = await findBestSeed(unlockedLandCount, { maxFootprint: 1 });
    } catch (e) {
        logWarn('商店', `查询失败: ${e.message}`);
        const uniqueFertilizeTargets = Array.from(new Set(fertilizeTargets));
        if (uniqueFertilizeTargets.length > 0) {
            const fertilized = await fertilize(uniqueFertilizeTargets);
            if (fertilized > 0) {
                log('施肥', `已为 ${fertilized}/${uniqueFertilizeTargets.length} 块地施肥`);
            }
        }
        return;
    }
    if (!bestSeed) {
        const uniqueFertilizeTargets = Array.from(new Set(fertilizeTargets));
        if (uniqueFertilizeTargets.length > 0) {
            const fertilized = await fertilize(uniqueFertilizeTargets);
            if (fertilized > 0) {
                log('施肥', `已为 ${fertilized}/${uniqueFertilizeTargets.length} 块地施肥`);
            }
        }
        return;
    }

    const seedName = getPlantNameBySeedId(bestSeed.seedId);
    const growTime = getPlantGrowTime(1020000 + (bestSeed.seedId - 20000));  // 转换为植物ID
    const growTimeStr = growTime > 0 ? ` 生长${formatGrowTime(growTime)}` : '';
    log('商店', `最佳种子: ${seedName} (${bestSeed.seedId}) 价格=${bestSeed.price}金币${growTimeStr}`);

    // 3. 购买
    const needCount = remainingLandIds.length;
    const totalCost = bestSeed.price * needCount;
    if (totalCost > state.gold) {
        logWarn('商店', `金币不足! 需要 ${totalCost} 金币, 当前 ${state.gold} 金币`);
        const canBuy = Math.floor(state.gold / bestSeed.price);
        if (canBuy <= 0) {
            const uniqueFertilizeTargets = Array.from(new Set(fertilizeTargets));
            if (uniqueFertilizeTargets.length > 0) {
                const fertilized = await fertilize(uniqueFertilizeTargets);
                if (fertilized > 0) {
                    log('施肥', `已为 ${fertilized}/${uniqueFertilizeTargets.length} 块地施肥`);
                }
            }
            return;
        }
        remainingLandIds = remainingLandIds.slice(0, canBuy);
        log('商店', `金币有限，只种 ${canBuy} 块地`);
    }

    let actualSeedId = bestSeed.seedId;
    try {
        const buyReply = await buyGoods(bestSeed.goodsId, remainingLandIds.length, bestSeed.price);
        if (buyReply.get_items && buyReply.get_items.length > 0) {
            const gotItem = buyReply.get_items[0];
            const gotId = toNum(gotItem.id);
            const gotCount = toNum(gotItem.count);
            log('购买', `获得物品: ${getItemName(gotId)}(${gotId}) x${gotCount}`);
            if (gotId > 0) actualSeedId = gotId;
        }
        if (buyReply.cost_items) {
            for (const item of buyReply.cost_items) {
                state.gold -= toNum(item.count);
            }
            updateStatusGold(state.gold);
        }
        const boughtName = getPlantNameBySeedId(actualSeedId);
        log('购买', `已购买 ${boughtName}种子 x${remainingLandIds.length}, 花费 ${bestSeed.price * remainingLandIds.length} 金币`);
    } catch (e) {
        logWarn('购买', e.message);
        const uniqueFertilizeTargets = Array.from(new Set(fertilizeTargets));
        if (uniqueFertilizeTargets.length > 0) {
            const fertilized = await fertilize(uniqueFertilizeTargets);
            if (fertilized > 0) {
                log('施肥', `已为 ${fertilized}/${uniqueFertilizeTargets.length} 块地施肥`);
            }
        }
        return;
    }

    // 4. 种植（逐块拖动，间隔50ms）
    const plantedActionLandIds = [...fertilizeTargets];
    try {
        const planted = await plantSeeds(actualSeedId, remainingLandIds);
        if (planted.cells > 0) {
            log('种植', `已在 ${planted.cells} 块地种植 (${planted.groups.map((group) => group.join(',')).join(' / ')})`);
            plantedActionLandIds.push(...planted.groups.map((group) => group[0]));
        }
    } catch (e) {
        logWarn('种植', e.message);
    }

    // 5. 施肥（逐块拖动，间隔50ms）
    if (plantedActionLandIds.length > 0) {
        const fertilized = await fertilize(Array.from(new Set(plantedActionLandIds)));
        if (fertilized > 0) {
            log('施肥', `已为 ${fertilized}/${Array.from(new Set(plantedActionLandIds)).length} 块地施肥`);
        }
    }
}

// ============ 土地分析 ============

/**
 * 根据服务器时间确定当前实际生长阶段
 */
function getCurrentPhase(phases, debug, landLabel) {
    if (!phases || phases.length === 0) return null;

    const nowSec = getServerTimeSec();

    if (debug) {
        console.log(`    ${landLabel} 服务器时间=${nowSec} (${new Date(nowSec * 1000).toLocaleTimeString()})`);
        for (let i = 0; i < phases.length; i++) {
            const p = phases[i];
            const bt = toTimeSec(p.begin_time);
            const phaseName = PHASE_NAMES[p.phase] || `阶段${p.phase}`;
            const diff = bt > 0 ? (bt - nowSec) : 0;
            const diffStr = diff > 0 ? `(未来 ${diff}s)` : diff < 0 ? `(已过 ${-diff}s)` : '';
            console.log(`    ${landLabel}   [${i}] ${phaseName}(${p.phase}) begin=${bt} ${diffStr} dry=${toTimeSec(p.dry_time)} weed=${toTimeSec(p.weeds_time)} insect=${toTimeSec(p.insect_time)}`);
        }
    }

    for (let i = phases.length - 1; i >= 0; i--) {
        const beginTime = toTimeSec(phases[i].begin_time);
        if (beginTime > 0 && beginTime <= nowSec) {
            if (debug) {
                console.log(`    ${landLabel}   → 当前阶段: ${PHASE_NAMES[phases[i].phase] || phases[i].phase}`);
            }
            return phases[i];
        }
    }

    if (debug) {
        console.log(`    ${landLabel}   → 所有阶段都在未来，使用第一个: ${PHASE_NAMES[phases[0].phase] || phases[0].phase}`);
    }
    return phases[0];
}

function clamp01(val) {
    if (!Number.isFinite(val)) return 0;
    if (val <= 0) return 0;
    if (val >= 1) return 1;
    return val;
}

function serializePhaseForWebui(phase) {
    if (!phase) return null;
    const phaseVal = Number.isFinite(Number(phase.phase)) ? Number(phase.phase) : 0;
    return {
        phase: phaseVal,
        phase_name: PHASE_NAMES[phaseVal] || `阶段${phaseVal}`,
        begin_time: toTimeSec(phase.begin_time),
        dry_time: toTimeSec(phase.dry_time),
        weeds_time: toTimeSec(phase.weeds_time),
        insect_time: toTimeSec(phase.insect_time),
    };
}

function serializePlantForWebui(plant, serverNowSec) {
    if (!plant) return null;
    const phases = Array.isArray(plant.phases) ? plant.phases : [];
    const currentPhase = getCurrentPhase(phases, false, '');
    const currentPhaseVal = currentPhase ? Number(currentPhase.phase) : 0;
    const phaseRows = phases.map(serializePhaseForWebui).filter(Boolean);

    let firstPhaseBeginSec = 0;
    let matureBeginSec = 0;
    for (const p of phaseRows) {
        if (p.begin_time > 0 && (firstPhaseBeginSec === 0 || p.begin_time < firstPhaseBeginSec)) {
            firstPhaseBeginSec = p.begin_time;
        }
        if (p.phase === PlantPhase.MATURE && p.begin_time > 0) {
            matureBeginSec = p.begin_time;
        }
    }

    let progressPct = null;
    if (matureBeginSec > 0 && firstPhaseBeginSec > 0 && matureBeginSec > firstPhaseBeginSec) {
        progressPct = Math.round(clamp01((serverNowSec - firstPhaseBeginSec) / (matureBeginSec - firstPhaseBeginSec)) * 1000) / 10;
    } else if (currentPhaseVal === PlantPhase.MATURE) {
        progressPct = 100;
    } else if (currentPhaseVal === PlantPhase.DEAD) {
        progressPct = 100;
    }

    const matureRemainingSec = matureBeginSec > 0 ? Math.max(0, matureBeginSec - serverNowSec) : null;
    const currentPhaseRow = serializePhaseForWebui(currentPhase);

    return {
        id: toNum(plant.id),
        name: plant.name || '',
        season: toNum(plant.season),
        grow_sec: toNum(plant.grow_sec),
        dry_num: toNum(plant.dry_num),
        stole_num: toNum(plant.stole_num),
        fruit_id: toNum(plant.fruit_id),
        fruit_num: toNum(plant.fruit_num),
        stealable: Boolean(plant.stealable),
        left_inorc_fert_times: toNum(plant.left_inorc_fert_times),
        left_fruit_num: toNum(plant.left_fruit_num),
        is_nudged: Boolean(plant.is_nudged),
        current_phase: currentPhaseVal,
        current_phase_name: PHASE_NAMES[currentPhaseVal] || `阶段${currentPhaseVal}`,
        current_phase_begin_time: currentPhaseRow ? currentPhaseRow.begin_time : 0,
        first_phase_begin_time: firstPhaseBeginSec || 0,
        mature_begin_time: matureBeginSec || 0,
        mature_remaining_sec: matureRemainingSec,
        progress_pct: progressPct,
        phase_count: phaseRows.length,
        phases: phaseRows,
    };
}

function serializeLandForWebui(land, slotIndex, serverNowSec) {
    if (!land) {
        return {
            slot: slotIndex + 1,
            id: slotIndex + 1,
            unlocked: false,
            missing: true,
        };
    }

    return {
        slot: slotIndex + 1,
        id: toNum(land.id),
        unlocked: Boolean(land.unlocked),
        level: toNum(land.level),
        max_level: toNum(land.max_level),
        could_unlock: Boolean(land.could_unlock),
        could_upgrade: Boolean(land.could_upgrade),
        land_size: toNum(land.land_size),
        lands_level: toNum(land.lands_level),
        is_shared: Boolean(land.is_shared),
        can_share: Boolean(land.can_share),
        master_land_id: toNum(land.master_land_id),
        slave_land_ids: Array.isArray(land.slave_land_ids) ? land.slave_land_ids.map(toNum) : [],
        plant: serializePlantForWebui(land.plant, serverNowSec),
    };
}

function writeWebuiLandsSnapshot(lands) {
    try {
        const serverNowSec = getServerTimeSec();
        const capturedAtMs = Date.now();
        const landRows = (Array.isArray(lands) ? lands : []).map((land, idx) => serializeLandForWebui(land, idx, serverNowSec));
        const payload = {
            version: 1,
            source: 'qq-farm-bot',
            updated_at: new Date(capturedAtMs).toISOString(),
            captured_at_ms: capturedAtMs,
            server_now_sec: serverNowSec,
            total: landRows.length,
            unlocked_count: landRows.filter(l => l && l.unlocked).length,
            lands: landRows,
        };

        fs.mkdirSync(path.dirname(WEBUI_LANDS_PATH), { recursive: true });
        const tmpPath = `${WEBUI_LANDS_PATH}.tmp`;
        fs.writeFileSync(tmpPath, `${JSON.stringify(payload)}\n`, 'utf8');
        fs.renameSync(tmpPath, WEBUI_LANDS_PATH);
        lastWebuiLandsWriteErrorMsg = '';
    } catch (e) {
        const now = Date.now();
        const msg = e && e.message ? e.message : String(e);
        if (msg !== lastWebuiLandsWriteErrorMsg || (now - lastWebuiLandsWriteErrorAt) > 60_000) {
            logWarn('WEBUI', `写入地块快照失败: ${msg}`);
            lastWebuiLandsWriteErrorMsg = msg;
            lastWebuiLandsWriteErrorAt = now;
        }
    }
}

function analyzeLands(lands) {
    const result = {
        harvestable: [], needWater: [], needWeed: [], needBug: [], needFertilize: [],
        growing: [], empty: [], dead: [], occupied: [],
        harvestableInfo: [],  // 收获植物的详细信息 { id, name, exp }
        soonToMature: [], // 即将成熟的地块，给秒收定时器使用
    };

    const nowSec = getServerTimeSec();
    const debug = false;
    const occupancy = buildLandOccupancyContext(lands);

    if (debug) {
        console.log('');
        console.log('========== 首次巡田详细日志 ==========');
        console.log(`  服务器时间(秒): ${nowSec}  (${new Date(nowSec * 1000).toLocaleString()})`);
        console.log(`  总土地数: ${lands.length}`);
        console.log('');
    }

    for (const land of lands) {
        const id = toNum(land.id);
        if (!land.unlocked) {
            if (debug) console.log(`  土地#${id}: 未解锁`);
            continue;
        }

        if (occupancy.occupiedSlaveIds.has(id)) {
            result.occupied.push(id);
            if (debug) {
                const masterLandId = toNum(land.master_land_id);
                console.log(`  土地#${id}: 被土地#${masterLandId} 占用`);
            }
            continue;
        }

        const plant = land.plant;
        if (plant && plant.name) {
            rememberLargeCropName(plant.name);
        }
        if (!plant || !plant.phases || plant.phases.length === 0) {
            result.empty.push(id);
            if (debug) console.log(`  土地#${id}: 空地`);
            continue;
        }

        const plantName = plant.name || '未知作物';
        const landLabel = `土地#${id}(${plantName})`;

        if (debug) {
            console.log(`  ${landLabel}: phases=${plant.phases.length} dry_num=${toNum(plant.dry_num)} weed_owners=${(plant.weed_owners||[]).length} insect_owners=${(plant.insect_owners||[]).length}`);
        }

        const currentPhase = getCurrentPhase(plant.phases, debug, landLabel);
        if (!currentPhase) {
            result.empty.push(id);
            continue;
        }
        const phaseVal = currentPhase.phase;

        if (phaseVal === PlantPhase.DEAD) {
            result.dead.push(id);
            if (debug) console.log(`    → 结果: 枯死`);
            continue;
        }

        if (phaseVal === PlantPhase.MATURE) {
            result.harvestable.push(id);
            // 收集植物信息用于日志
            const plantId = toNum(plant.id);
            const plantNameFromConfig = getPlantName(plantId);
            const plantExp = getPlantExp(plantId);
            result.harvestableInfo.push({
                landId: id,
                plantId,
                name: plantNameFromConfig || plantName,
                exp: plantExp,
            });
            if (debug) console.log(`    → 结果: 可收获 (${plantNameFromConfig} +${plantExp}经验)`);
            continue;
        }

        let landNeeds = [];
        const dryNum = toNum(plant.dry_num);
        const dryTime = toTimeSec(currentPhase.dry_time);
        if (dryNum > 0 || (dryTime > 0 && dryTime <= nowSec)) {
            result.needWater.push(id);
            landNeeds.push('缺水');
        }

        const weedsTime = toTimeSec(currentPhase.weeds_time);
        const hasWeeds = (plant.weed_owners && plant.weed_owners.length > 0) || (weedsTime > 0 && weedsTime <= nowSec);
        if (hasWeeds) {
            result.needWeed.push(id);
            landNeeds.push('有草');
        }

        const insectTime = toTimeSec(currentPhase.insect_time);
        const hasBugs = (plant.insect_owners && plant.insect_owners.length > 0) || (insectTime > 0 && insectTime <= nowSec);
        if (hasBugs) {
            result.needBug.push(id);
            landNeeds.push('有虫');
        }

        const fertTimesLeft = Math.max(0, Math.floor(toNum(plant.left_inorc_fert_times)));
        if (fertTimesLeft > 0) {
            result.needFertilize.push(id);
            landNeeds.push(`可施肥x${fertTimesLeft}`);
        }

        const maturePhase = plant.phases.find((p) => toNum(p.phase) === PlantPhase.MATURE);
        if (farmFastHarvestEnabled && maturePhase) {
            const matureBegin = toTimeSec(maturePhase.begin_time);
            const diff = matureBegin - nowSec;
            if (diff > 0 && diff <= FAST_HARVEST_PREPARE_WINDOW_SEC) {
                result.soonToMature.push({
                    landId: id,
                    plantId: toNum(plant.id),
                    plantName: getPlantName(toNum(plant.id)) || plantName,
                    matureTime: matureBegin,
                });
            }
        }

        result.growing.push(id);
        if (debug) {
            const needStr = landNeeds.length > 0 ? ` 需要: ${landNeeds.join(',')}` : '';
            console.log(`    → 结果: 生长中(${PHASE_NAMES[phaseVal] || phaseVal})${needStr}`);
        }
    }

    if (debug) {
        console.log('');
        console.log('========== 巡田分析汇总 ==========');
        console.log(`  可收获: ${result.harvestable.length} [${result.harvestable.join(',')}]`);
        console.log(`  生长中: ${result.growing.length} [${result.growing.join(',')}]`);
        console.log(`  缺水:   ${result.needWater.length} [${result.needWater.join(',')}]`);
        console.log(`  有草:   ${result.needWeed.length} [${result.needWeed.join(',')}]`);
        console.log(`  有虫:   ${result.needBug.length} [${result.needBug.join(',')}]`);
        console.log(`  可肥:   ${result.needFertilize.length} [${result.needFertilize.join(',')}]`);
        console.log(`  空地:   ${result.empty.length} [${result.empty.join(',')}]`);
        console.log(`  枯死:   ${result.dead.length} [${result.dead.join(',')}]`);
        console.log('====================================');
        console.log('');
    }

    return result;
}

async function tryUpgradeAllLandsOnce(lands, reason = 'manual') {
    if (isLandUpgradeSweepRunning) {
        return { skipped: true, reason: 'busy', refreshedLands: null };
    }

    isLandUpgradeSweepRunning = true;
    try {
        let workingLands = Array.isArray(lands) ? lands : [];
        let unlockedCount = workingLands.filter((land) => land && land.unlocked).length;
        log(
            '升级土地',
            unlockedCount < FARM_UNLOCK_LAND_MAX_ID
                ? `开始(${reason}): 已解锁${unlockedCount}/${FARM_UNLOCK_LAND_MAX_ID}块，按抓包逻辑持续尝试扩建`
                : `开始(${reason}): 已解锁${unlockedCount}/${FARM_UNLOCK_LAND_MAX_ID}块，开始尝试升级等级`
        );

        let success = 0;
        let fail = 0;
        let attempted = 0;
        const touchedIds = [];
        const codeStats = new Map();
        const reasonStats = new Map();
        const failSamples = [];
        let skippedUpgradeBecauseNotFull = false;
        const refreshWorkingLands = async (warnText = '') => {
            try {
                const refreshed = await getAllLands();
                if (refreshed && Array.isArray(refreshed.lands) && refreshed.lands.length > 0) {
                    workingLands = refreshed.lands;
                }
            } catch (e) {
                if (warnText) {
                    logWarn('升级土地', `${warnText}: ${e.message || String(e)}`);
                }
            }
            return workingLands;
        };
        const collectFail = (landId, err) => {
            fail++;
            const code = extractErrorCode(err);
            if (code > 0) codeStats.set(code, (codeStats.get(code) || 0) + 1);
            const reasonLabel = getLandUpgradeErrorLabel(err);
            if (reasonLabel) {
                reasonStats.set(reasonLabel, (reasonStats.get(reasonLabel) || 0) + 1);
            }
            if (!reasonLabel && failSamples.length < 3) {
                const msg = normalizeUpgradeErrorText(err).slice(0, 140);
                failSamples.push(`#${landId}${code > 0 ? ` code=${code}` : ''}${msg ? ` ${msg}` : ''}`);
            }
        };

        // 阶段1：未满 24 块时只做扩建，并持续尝试到无法继续为止。
        let unlockRounds = 0;
        while (unlockedCount < FARM_UNLOCK_LAND_MAX_ID && unlockRounds < FARM_UNLOCK_LAND_MAX_ID) {
            unlockRounds++;
            const nextLockedLand = workingLands
                .filter((land) => land && !land.unlocked && toNum(land.id) > 0)
                .sort((a, b) => toNum(a.id) - toNum(b.id))[0];
            const landId = nextLockedLand ? toNum(nextLockedLand.id) : Math.min(unlockedCount + 1, FARM_UNLOCK_LAND_MAX_ID);
            if (landId > 0 && landId <= FARM_UNLOCK_LAND_MAX_ID) {
                attempted++;
                try {
                    await unlockLand(landId);
                    await sleep(80);
                    await refreshWorkingLands('解锁后刷新地块快照失败');

                    const beforeUnlockedCount = unlockedCount;
                    const afterUnlockedCount = workingLands.filter((land) => land && land.unlocked).length;
                    if (afterUnlockedCount <= beforeUnlockedCount) {
                        const err = new Error('解锁未生效');
                        collectFail(landId, err);
                        logWarn('升级土地', `解锁地块#${landId} 未生效，停止继续解锁`);
                        break;
                    } else {
                        unlockedCount = afterUnlockedCount;
                        success++;
                        touchedIds.push(`#${landId}(解锁)`);
                        log('升级土地', `解锁地块#${landId} 成功（已解锁${unlockedCount}/${FARM_UNLOCK_LAND_MAX_ID}）`);
                        if (unlockedCount < FARM_UNLOCK_LAND_MAX_ID) {
                            await sleep(80);
                        }
                    }
                } catch (e) {
                    collectFail(landId, e);
                    logWarn('升级土地', `解锁地块#${landId} 失败: ${normalizeUpgradeErrorText(e)}（停止继续解锁）`);
                    break;
                }
            } else {
                break;
            }
        }

        // 阶段2：只有满 24 块后才尝试升级土地等级，并持续扫到做不了为止。
        if (unlockedCount < FARM_UNLOCK_LAND_MAX_ID) {
            skippedUpgradeBecauseNotFull = true;
            log('升级土地', `当前仅解锁${unlockedCount}/${FARM_UNLOCK_LAND_MAX_ID}块，跳过升级阶段`);
        }

        let upgradePass = 0;
        while (unlockedCount >= FARM_UNLOCK_LAND_MAX_ID && upgradePass < 32) {
            upgradePass++;
            const upgradeCandidates = workingLands
                .filter((land) => land && land.unlocked && toNum(land.id) > 0)
                .filter((land) => {
                    const level = toNum(land.level);
                    const maxLevel = Math.max(level, toNum(land.max_level));
                    return maxLevel > 0 && level < maxLevel;
                })
                .sort((a, b) => {
                    const aHint = a.could_upgrade ? 0 : 1;
                    const bHint = b.could_upgrade ? 0 : 1;
                    if (aHint !== bHint) return aHint - bHint;
                    const lvDiff = toNum(a.level) - toNum(b.level);
                    if (lvDiff !== 0) return lvDiff;
                    return toNum(a.id) - toNum(b.id);
                });

            if (upgradeCandidates.length <= 0) {
                break;
            }

            log('升级土地', `升级阶段#${upgradePass}: 候选${upgradeCandidates.length}块`);
            let passSuccess = 0;
            for (const land of upgradeCandidates) {
                const landId = toNum(land.id);
                if (landId <= 0) continue;
                attempted++;
                try {
                    await upgradeLand(landId);
                    success++;
                    passSuccess++;
                    touchedIds.push(`#${landId}(升级)`);
                } catch (e) {
                    collectFail(landId, e);
                }
                if (upgradeCandidates.length > 1) await sleep(80);
            }

            if (passSuccess <= 0) {
                break;
            }

            await sleep(80);
            await refreshWorkingLands('升级后刷新地块快照失败');
        }

        let refreshedLands = workingLands;
        if (success > 0) {
            refreshedLands = await refreshWorkingLands('刷新地块快照失败');
        }

        if (attempted === 0) {
            log(
                '升级土地',
                skippedUpgradeBecauseNotFull
                    ? `跳过(${reason}): 仅解锁${unlockedCount}/${FARM_UNLOCK_LAND_MAX_ID}块`
                    : `跳过(${reason}): 无可解锁/可升级地块`
            );
            return { success: 0, fail: 0, skipped: false, refreshedLands };
        }

        const codeSummary = [...codeStats.entries()]
            .sort((a, b) => a[0] - b[0])
            .map(([code, count]) => `${code}x${count}`)
            .join(',');
        const reasonSummary = [...reasonStats.entries()]
            .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], 'zh-CN'))
            .map(([label, count]) => `${label}${count}次`)
            .join('，');
        const knownReasonFailTotal = [...reasonStats.values()].reduce((sum, n) => sum + Number(n || 0), 0);
        const unknownFailCount = Math.max(0, fail - knownReasonFailTotal);
        const failReasonSummaryText = [
            reasonSummary,
            unknownFailCount > 0 ? `未知原因${unknownFailCount}次` : '',
        ].filter(Boolean).join('，');
        const upgradedText = touchedIds.length > 0 ? ` 成功地块=${touchedIds.slice(0, 8).join(',')}${touchedIds.length > 8 ? '…' : ''}` : '';
        if (success > 0) {
            log(
                '升级土地',
                `完成(${reason}): 候选${attempted}次，成功${success}次，未成功${fail}次`
                + `${failReasonSummaryText ? `（${failReasonSummaryText}）` : ''}${upgradedText}`
            );
        } else {
            log(
                '升级土地',
                `完成(${reason}): 本轮未成功（候选${attempted}次`
                + `${failReasonSummaryText ? `；${failReasonSummaryText}` : `；失败${fail}次`}`
                + `${!failReasonSummaryText && codeSummary ? `；错误码=${codeSummary}` : ''}）`
            );
        }
        if (failSamples.length > 0) {
            logWarn('升级土地', `未知失败示例: ${failSamples.join(' | ')}${codeSummary ? ` | 错误码统计=${codeSummary}` : ''}`);
        }

        return { success, fail, skipped: false, refreshedLands };
    } finally {
        isLandUpgradeSweepRunning = false;
    }
}

// ============ 巡田主循环 ============

async function checkFarm() {
    const state = getUserState();
    if (isCheckingFarm || !state.gid) return false;
    isCheckingFarm = true;
    let checkTimedOut = false;

    try {
        maybeQueueDailyMallClaim();
        applyRuntimeFarmConfig();
        await runPendingMallAndItemActions();
        if (farmAutoFertilizeEnabled) {
            await maybeMaintainNormalFertilizerReserve('loop');
        }
        await reportFertilizerStatusIfStale();

        const landsReply = await getAllLands();
        if (!landsReply.lands || landsReply.lands.length === 0) {
            log('农场', '没有土地数据');
            farmLoopTimeoutBackoffMs = 0;
            return false;
        }

        let lands = landsReply.lands;

        if (pendingLandUpgradeSweep) {
            const sweepReason = pendingLandUpgradeSweepReason || 'manual';
            pendingLandUpgradeSweep = false;
            pendingLandUpgradeSweepReason = '';
            if (/level-up|manual-button/i.test(sweepReason)) {
                const unlockedForCalc = lands.filter((land) => land && land.unlocked).length;
                applyPreferredSeedTargetByNormalFert(state.level, unlockedForCalc, `before-upgrade ${sweepReason}`);
            }
            const sweepResult = await tryUpgradeAllLandsOnce(lands, sweepReason);
            if (sweepResult && Array.isArray(sweepResult.refreshedLands) && sweepResult.refreshedLands.length > 0) {
                lands = sweepResult.refreshedLands;
            }
            if (sweepResult && sweepResult.success > 0 && /level-up|manual-button/i.test(sweepReason)) {
                const unlockedForCalc = lands.filter((land) => land && land.unlocked).length;
                applyPreferredSeedTargetByNormalFert(state.level, unlockedForCalc, `after-upgrade ${sweepReason}`);
            }
        }

        writeWebuiLandsSnapshot(lands);
        const status = analyzeLands(lands);
        syncFastHarvestTimers(status.soonToMature);
        const unlockedLandCount = lands.filter(land => land && land.unlocked).length;
        isFirstFarmCheck = false;

        // 构建状态摘要
        const statusParts = [];
        if (status.harvestable.length) statusParts.push(`收:${status.harvestable.length}`);
        if (status.needWeed.length) statusParts.push(`草:${status.needWeed.length}`);
        if (status.needBug.length) statusParts.push(`虫:${status.needBug.length}`);
        if (status.needWater.length) statusParts.push(`水:${status.needWater.length}`);
        if (status.needFertilize.length) statusParts.push(`肥:${status.needFertilize.length}`);
        if (status.dead.length) statusParts.push(`枯:${status.dead.length}`);
        if (status.empty.length) statusParts.push(`空:${status.empty.length}`);
        statusParts.push(`长:${status.growing.length}`);

        const shouldAutoFertilize = farmAutoFertilizeEnabled
            && status.needFertilize.length > 0
            && (!Number.isFinite(lastKnownNormalFertilizerSec) || lastKnownNormalFertilizerSec > 0);
        const hasWork = status.harvestable.length || status.needWeed.length || status.needBug.length
            || status.needWater.length || shouldAutoFertilize || status.dead.length || status.empty.length;

        // 执行操作并收集结果
        const actions = [];

        // 一键操作：除草、除虫、浇水可以并行执行（游戏中都是一键完成）
        const batchOps = [];
        if (status.needWeed.length > 0) {
            batchOps.push(weedOut(status.needWeed).then(() => actions.push(`除草${status.needWeed.length}`)).catch(e => logActionError('除草', e)));
        }
        if (status.needBug.length > 0) {
            batchOps.push(insecticide(status.needBug).then(() => actions.push(`除虫${status.needBug.length}`)).catch(e => logActionError('除虫', e)));
        }
        if (status.needWater.length > 0) {
            batchOps.push(waterLand(status.needWater).then(() => actions.push(`浇水${status.needWater.length}`)).catch(e => logActionError('浇水', e)));
        }
        if (batchOps.length > 0) {
            await Promise.all(batchOps);
        }
        if (shouldAutoFertilize) {
            try {
                const fertilized = await fertilize(Array.from(new Set(status.needFertilize)));
                if (fertilized > 0) {
                    actions.push(`施肥${fertilized}`);
                }
            } catch (e) {
                logActionError('施肥', e);
            }
        }

        // 收获（一键操作）
        let harvestedLandIds = [];
        let postHarvestStatus = null;
        let postHarvestLands = null;
        if (status.harvestable.length > 0) {
            try {
                for (const landId of status.harvestable) {
                    clearFastHarvestTimer(landId);
                }
                await harvest(status.harvestable);
                scheduleIllustratedClaim(600);
                actions.push(`收获${status.harvestable.length}`);
                harvestedLandIds = [...status.harvestable];

                // 关键：收获后立刻刷新土地，再决定是否铲除/重种。
                // 这样多季作物首季收获后若进入下一季生长态，不会被误铲。
                const refreshedAfterHarvest = await getAllLands();
                if (refreshedAfterHarvest && Array.isArray(refreshedAfterHarvest.lands) && refreshedAfterHarvest.lands.length > 0) {
                    postHarvestLands = refreshedAfterHarvest.lands;
                    postHarvestStatus = analyzeLands(postHarvestLands);
                    syncFastHarvestTimers(postHarvestStatus.soonToMature);
                    writeWebuiLandsSnapshot(postHarvestLands);

                    const keepGrowingIds = harvestedLandIds
                        .filter((id) => !postHarvestStatus.dead.includes(id) && !postHarvestStatus.empty.includes(id));
                    if (keepGrowingIds.length > 0) {
                        log('收获', `检测到继续生长地块 ${keepGrowingIds.length} 块 (${keepGrowingIds.join(',')})，本轮跳过铲除`);
                    }
                }
            } catch (e) { logWarn('收获', e.message); }
        }

        // 铲除 + 种植 + 施肥（需要顺序执行）
        const replantStatus = postHarvestStatus || status;
        const replantLands = Array.isArray(postHarvestLands) && postHarvestLands.length > 0 ? postHarvestLands : lands;
        const replantUnlockedLandCount = replantLands.filter(land => land && land.unlocked).length;
        const allDeadLands = [...replantStatus.dead];
        const allEmptyLands = [...replantStatus.empty];
        if (allDeadLands.length > 0 || allEmptyLands.length > 0) {
            try {
                await autoPlantEmptyLands(allDeadLands, allEmptyLands, replantUnlockedLandCount);
                actions.push(`种植${allDeadLands.length + allEmptyLands.length}`);
            } catch (e) { logWarn('种植', e.message); }
        }

        if (farmAutoFertilizeEnabled && postHarvestStatus && postHarvestStatus.needFertilize.length > 0) {
            try {
                const fertilizeTargets = Array.from(new Set(postHarvestStatus.needFertilize));
                const fertilized = await fertilize(fertilizeTargets);
                if (fertilized > 0) {
                    actions.push(`补肥${fertilized}`);
                }
            } catch (e) {
                logActionError('施肥', e);
            }
        }

        // 输出一行日志
        const actionStr = actions.length > 0 ? ` → ${actions.join('/')}` : '';
        if(hasWork) {
            log('农场', `[${statusParts.join(' ')}]${actionStr}${!hasWork ? ' 无需操作' : ''}`)
        }
        farmLoopTimeoutBackoffMs = 0;
    } catch (err) {
        const msg = sanitizeLogText((err && err.message) || err || '');
        checkTimedOut = /请求超时|request timeout|timeout/i.test(msg);
        if (checkTimedOut) {
            farmLoopTimeoutBackoffMs = farmLoopTimeoutBackoffMs > 0
                ? Math.min(farmLoopTimeoutBackoffMs * 2, LOOP_TIMEOUT_BACKOFF_MAX_MS)
                : LOOP_TIMEOUT_BACKOFF_BASE_MS;
            logWarn('巡田', `检查失败: ${msg}（临时退避 ${farmLoopTimeoutBackoffMs}ms）`);
        } else {
            farmLoopTimeoutBackoffMs = 0;
            logWarn('巡田', `检查失败: ${msg}`);
        }
    } finally {
        isCheckingFarm = false;
    }
    return checkTimedOut;
}

/**
 * 农场巡查循环 - 本次完成后等待指定秒数再开始下次
 */
async function farmCheckLoop() {
    while (farmLoopRunning) {
        await checkFarm();
        if (!farmLoopRunning) break;
        const minLoopWaitMs = (Number(CONFIG.farmCheckInterval) || 0) <= 0 ? FARM_MIN_LOOP_WAIT_WHEN_ZERO_MS : 0;
        const networkGuardWaitMs = getFarmNetworkGuardWaitMs();
        const waitMs = Math.max(
            Number(CONFIG.farmCheckInterval) || 0,
            farmLoopTimeoutBackoffMs,
            minLoopWaitMs,
            networkGuardWaitMs
        );
        if (waitMs > 0) {
            await sleep(waitMs);
        }
    }
}

function startFarmCheckLoop() {
    if (farmLoopRunning) return;
    farmLoopRunning = true;

    if (!startupMallDailyClaimQueued) {
        startupMallDailyClaimQueued = true;
        queueMallDailyClaim('startup');
        const now = new Date();
        const reached = now.getHours() > MALL_DAILY_AUTO_HOUR
            || (now.getHours() === MALL_DAILY_AUTO_HOUR && now.getMinutes() >= MALL_DAILY_AUTO_MINUTE);
        if (reached) {
            lastMallDailyAutoClaimDateKey = getLocalDateKey(now);
        }
    }

    // 监听服务器推送的土地变化事件
    networkEvents.on('landsChanged', onLandsChangedPush);
    networkEvents.on('userLevelChanged', onUserLevelChanged);

    // 延迟 2 秒后启动循环
    farmCheckTimer = setTimeout(() => farmCheckLoop(), 2000);
}

/**
 * 处理服务器推送的土地变化
 */
let lastPushTime = 0;
function onLandsChangedPush(lands) {
    if (isCheckingFarm) return;
    const now = Date.now();
    if (now - lastPushTime < 500) return;  // 500ms 防抖
    
    lastPushTime = now;
    log('农场', `收到推送: ${lands.length}块土地变化，检查中...`);
    
    setTimeout(async () => {
        if (!isCheckingFarm) {
            await checkFarm();
        }
    }, 100);
}

function onUserLevelChanged(payload) {
    const oldLevel = toNum(payload && payload.oldLevel);
    const newLevel = toNum(payload && payload.newLevel);
    if (newLevel > oldLevel && oldLevel > 0) {
        applyLevelUpPreferredSeedTarget(newLevel, oldLevel);
        queueLandUpgradeSweep(`level-up Lv${oldLevel}->Lv${newLevel}`);
    }
}

function stopFarmCheckLoop() {
    farmLoopRunning = false;
    startupMallDailyClaimQueued = false;
    clearAllFastHarvestTimers();
    if (farmCheckTimer) { clearTimeout(farmCheckTimer); farmCheckTimer = null; }
    networkEvents.removeListener('landsChanged', onLandsChangedPush);
    networkEvents.removeListener('userLevelChanged', onUserLevelChanged);
}

module.exports = {
    checkFarm, startFarmCheckLoop, stopFarmCheckLoop,
    getCurrentPhase,
    setOperationLimitsCallback,
    requestLandUpgradeSweep: queueLandUpgradeSweep,
    tryUpgradeAllLandsOnce,
};


