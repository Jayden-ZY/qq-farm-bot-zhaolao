/**
 * Task system - auto claim normal tasks and illustrated rewards.
 */

const { types } = require('./proto');
const { sendMsgAsync, networkEvents } = require('./network');
const { toLong, toNum, log, logWarn, sleep } = require('./utils');
const { getItemName } = require('./gameConfig');
const { getBag, getBagItems } = require('./warehouse');

let illustratedClaimTimer = null;

function extractErrorCode(err) {
    const msg = String((err && err.message) || err || '');
    const m = msg.match(/code=(\d+)/);
    return m ? Number(m[1]) : 0;
}

function shouldIgnoreTaskClaimError(err) {
    const code = extractErrorCode(err);
    return code === 1008001 || code === 1008002;
}

async function getTaskInfo() {
    const body = types.TaskInfoRequest.encode(types.TaskInfoRequest.create({})).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.taskpb.TaskService', 'TaskInfo', body);
    return types.TaskInfoReply.decode(replyBody);
}

async function claimTaskReward(taskId, doShared = false) {
    const body = types.ClaimTaskRewardRequest.encode(types.ClaimTaskRewardRequest.create({
        id: toLong(taskId),
        do_shared: doShared,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.taskpb.TaskService', 'ClaimTaskReward', body);
    return types.ClaimTaskRewardReply.decode(replyBody);
}

async function batchClaimTaskReward(taskIds, doShared = false) {
    const body = types.BatchClaimTaskRewardRequest.encode(types.BatchClaimTaskRewardRequest.create({
        ids: taskIds.map(id => toLong(id)),
        do_shared: doShared,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.taskpb.TaskService', 'BatchClaimTaskReward', body);
    return types.BatchClaimTaskRewardReply.decode(replyBody);
}

async function claimAllIllustratedRewards() {
    const body = types.ClaimAllRewardsV2Request.encode(types.ClaimAllRewardsV2Request.create({
        only_claimable: true,
    })).finish();
    const { body: replyBody } = await sendMsgAsync('gamepb.illustratedpb.IllustratedService', 'ClaimAllRewardsV2', body);
    return types.ClaimAllRewardsV2Reply.decode(replyBody);
}

function analyzeTaskList(tasks) {
    const claimable = [];
    for (const task of tasks) {
        const id = toNum(task.id);
        const progress = toNum(task.progress);
        const totalProgress = toNum(task.total_progress);
        const isClaimed = task.is_claimed;
        const isUnlocked = task.is_unlocked;
        const shareMultiple = toNum(task.share_multiple);

        if (isUnlocked && !isClaimed && progress >= totalProgress && totalProgress > 0) {
            claimable.push({
                id,
                desc: task.desc || `任务#${id}`,
                shareMultiple,
                rewards: task.rewards || [],
            });
        }
    }
    return claimable;
}

function getRewardSummary(items) {
    const summary = [];
    for (const item of items) {
        const id = toNum(item.id);
        const count = toNum(item.count);
        if (id === 1 || id === 1001) summary.push(`金币${count}`);
        else if (id === 2 || id === 1101) summary.push(`经验${count}`);
        else if (id === 1002) summary.push(`点券${count}`);
        else summary.push(`${getItemName(id)}(${id})x${count}`);
    }
    return summary.join('/');
}

async function getTicketBalanceFromBag() {
    try {
        const items = getBagItems(await getBag());
        for (const item of items) {
            if (toNum(item && item.id) === 1002) {
                return Math.max(0, toNum(item && item.count));
            }
        }
    } catch (e) {
        return 0;
    }
    return 0;
}

async function checkAndClaimIllustratedRewards() {
    if (!types.ClaimAllRewardsV2Request || !types.ClaimAllRewardsV2Reply) return false;

    try {
        const beforeTicket = await getTicketBalanceFromBag();
        const reply = await claimAllIllustratedRewards();
        const items = [
            ...(reply.items || []),
            ...(reply.bonus_items || []),
        ];
        const afterTicket = await getTicketBalanceFromBag();
        const gainTicket = Math.max(0, afterTicket - beforeTicket);

        if (items.length === 0 && gainTicket === 0) return false;

        const rewardStr = items.length > 0 ? getRewardSummary(items) : `点券${gainTicket}`;
        log('任务', `领取图鉴奖励: ${rewardStr}`);
        return true;
    } catch (e) {
        return false;
    }
}

function scheduleIllustratedClaim(delay = 1000) {
    if (illustratedClaimTimer) {
        clearTimeout(illustratedClaimTimer);
    }

    illustratedClaimTimer = setTimeout(async () => {
        illustratedClaimTimer = null;
        await checkAndClaimIllustratedRewards();
    }, delay);
}

async function checkAndClaimTasks() {
    try {
        const reply = await getTaskInfo();
        if (!reply.task_info) return;

        const taskInfo = reply.task_info;
        const allTasks = [
            ...(taskInfo.growth_tasks || []),
            ...(taskInfo.daily_tasks || []),
            ...(taskInfo.tasks || []),
        ];

        const claimable = analyzeTaskList(allTasks);
        if (claimable.length > 0) {
            log('任务', `发现 ${claimable.length} 个可领取任务`);
            await claimTasksFromList(claimable);
        }

        await checkAndClaimIllustratedRewards();
    } catch (e) {
        // keep silent
    }
}

function onTaskInfoNotify(taskInfo) {
    if (!taskInfo) return;

    const allTasks = [
        ...(taskInfo.growth_tasks || []),
        ...(taskInfo.daily_tasks || []),
        ...(taskInfo.tasks || []),
    ];

    const claimable = analyzeTaskList(allTasks);
    if (claimable.length === 0) {
        scheduleIllustratedClaim();
        return;
    }

    log('任务', `有 ${claimable.length} 个任务可领取，准备自动领取...`);
    setTimeout(async () => {
        await claimTasksFromList(claimable);
        await checkAndClaimIllustratedRewards();
    }, 1000);
}

function onIllustratedRewardNotify() {
    scheduleIllustratedClaim(800);
}

async function claimTasksFromList(claimable) {
    for (const task of claimable) {
        try {
            const useShare = task.shareMultiple > 1;
            const multipleStr = useShare ? ` (${task.shareMultiple}倍)` : '';

            const claimReply = await claimTaskReward(task.id, useShare);
            const items = claimReply.items || [];
            const rewardStr = items.length > 0 ? getRewardSummary(items) : '无';

            log('任务', `领取: ${task.desc}${multipleStr} -> ${rewardStr}`);
            await sleep(300);
        } catch (e) {
            if (!shouldIgnoreTaskClaimError(e)) {
                logWarn('任务', `领取失败 #${task.id}: ${e.message}`);
            }
        }
    }
}

function initTaskSystem() {
    cleanupTaskSystem();
    networkEvents.on('taskInfoNotify', onTaskInfoNotify);
    networkEvents.on('illustratedRewardNotify', onIllustratedRewardNotify);
    setTimeout(() => checkAndClaimTasks(), 4000);
}

function cleanupTaskSystem() {
    networkEvents.off('taskInfoNotify', onTaskInfoNotify);
    networkEvents.off('illustratedRewardNotify', onIllustratedRewardNotify);
    if (illustratedClaimTimer) {
        clearTimeout(illustratedClaimTimer);
        illustratedClaimTimer = null;
    }
}

module.exports = {
    batchClaimTaskReward,
    checkAndClaimTasks,
    checkAndClaimIllustratedRewards,
    initTaskSystem,
    cleanupTaskSystem,
};
