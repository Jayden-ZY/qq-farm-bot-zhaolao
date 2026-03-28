const protobuf = require('protobufjs');

const WARMUP_STEP_GAP_MS = 40;
const WARMUP_REQUEST_TIMEOUT_MS = 4000;
const PASSIVE_SYNC_REQUEST_TIMEOUT_MS = 3000;

let passiveSyncTimer = null;

function encodeGetRechargeInfoRequest(scene = 'MallUI') {
    const writer = protobuf.Writer.create();
    writer.uint32(10).string(String(scene || 'MallUI'));
    return writer.finish();
}

function encodeSetDisplayInfo(profile = {}) {
    const writer = protobuf.Writer.create();
    const name = String((profile && profile.name) || '').trim();
    const avatarUrl = String((profile && profile.avatarUrl) || '').trim();
    if (name) writer.uint32(10).string(name);
    if (avatarUrl) writer.uint32(18).string(avatarUrl);
    return writer.finish();
}

async function sendBestEffort(sendMsgAsync, serviceName, methodName, body, timeoutMs) {
    try {
        await sendMsgAsync(serviceName, methodName, body, timeoutMs);
        return true;
    } catch (e) {
        return false;
    }
}

async function runPassiveSessionSync(sendMsgAsync) {
    await Promise.allSettled([
        sendBestEffort(
            sendMsgAsync,
            'gamepb.dogpb.DogService',
            'GetDogInfo',
            Buffer.alloc(0),
            PASSIVE_SYNC_REQUEST_TIMEOUT_MS
        ),
        sendBestEffort(
            sendMsgAsync,
            'gamepb.sharepb.ShareService',
            'GetInviteInfo',
            Buffer.alloc(0),
            PASSIVE_SYNC_REQUEST_TIMEOUT_MS
        ),
    ]);
}

function ensurePassiveSessionSync(sendMsgAsync, intervalMs) {
    const safeIntervalMs = Math.max(30 * 1000, Number(intervalMs) || 0);
    if (!safeIntervalMs || passiveSyncTimer) return;
    passiveSyncTimer = setInterval(() => {
        void runPassiveSessionSync(sendMsgAsync);
    }, safeIntervalMs);
}

async function runLoginWarmup({ sendMsgAsync, types, sleep, profile, passiveSyncIntervalMs }) {
    const steps = [
        {
            // Mirror the captured startup flow: fetch the friend summary early.
            serviceName: 'gamepb.friendpb.FriendService',
            methodName: 'GetAll',
            body: types.GetAllFriendsRequest.encode(types.GetAllFriendsRequest.create({})).finish(),
        },
        {
            serviceName: 'gamepb.plantpb.PlantService',
            methodName: 'AllLands',
            body: types.AllLandsRequest.encode(types.AllLandsRequest.create({})).finish(),
        },
        {
            serviceName: 'gamepb.dogpb.DogService',
            methodName: 'GetDogInfo',
            body: Buffer.alloc(0),
        },
        {
            serviceName: 'gamepb.taskpb.TaskService',
            methodName: 'TaskInfo',
            body: types.TaskInfoRequest.encode(types.TaskInfoRequest.create({})).finish(),
        },
        {
            serviceName: 'gamepb.paypb.PayService',
            methodName: 'GetRechargeInfo',
            body: encodeGetRechargeInfoRequest('MallUI'),
        },
        {
            serviceName: 'gamepb.sharepb.ShareService',
            methodName: 'GetInviteInfo',
            body: Buffer.alloc(0),
        },
        {
            serviceName: 'gamepb.userpb.UserService',
            methodName: 'GetUserSettings',
            body: Buffer.alloc(0),
        },
    ];

    const setDisplayInfoBody = encodeSetDisplayInfo(profile);
    if (setDisplayInfoBody.length > 0) {
        steps.push({
            serviceName: 'gamepb.userpb.UserService',
            methodName: 'SetDisplayInfo',
            body: setDisplayInfoBody,
        });
    }

    const pending = [];
    for (let i = 0; i < steps.length; i++) {
        if (i > 0) await sleep(WARMUP_STEP_GAP_MS);
        const step = steps[i];
        pending.push(
            sendBestEffort(
                sendMsgAsync,
                step.serviceName,
                step.methodName,
                step.body,
                WARMUP_REQUEST_TIMEOUT_MS
            )
        );
    }

    await Promise.allSettled(pending);
    ensurePassiveSessionSync(sendMsgAsync, passiveSyncIntervalMs);
}

module.exports = {
    runLoginWarmup,
};
