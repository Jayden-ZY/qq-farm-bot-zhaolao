const state = {
    config: null,
    runtime: null,
    logs: [],
    plantOptions: [],
    landsPayload: null,
    landsSnapshot: null,
    landsFetchedAt: 0,
    landsRefreshTimer: null,
    landsRenderTicker: null,
    landsRequestId: 0,
    activeTag: 'ALL',
    search: '',
    uptimeTickHandle: null,
    farmCalcRefreshTimer: null,
    farmCalcRequestId: 0,
    farmCalcInputSig: '',
    farmCalcResult: null,
    friendInsights: null,
    friendInsightsRequestId: 0,
    friendInsightsRefreshTimer: null,
    friendSort: {
        key: 'value',
        order: 'desc',
    },
    friendFilters: {
        search: '',
        onlyFailed: false,
        onlyEffective: false,
    },
    friendPage: 1,
    friendPageSize: 20,
    logFilterSignature: '',
    configAutoSaveTimer: null,
    configSaveInFlight: false,
    configSaveQueued: false,
    configSavePendingReason: '',
    codeModalOpen: false,
    mallBuyModalOpen: false,
    bagItemsModalOpen: false,
    bagItemsLoading: false,
    bagItemsSnapshot: null,
    bagItemsSelection: new Set(),
    warehouseActiveTab: 'fruits',
    mallBuyPending: null,
    codeAuto: {
        loading: false,
        polling: false,
        pollTimer: null,
        status: null,
        showGuide: false,
    },
    expEstimator: {
        signature: '',
        lastCurrent: null,
        samples: [],
        smoothedRatePerHour: null,
        smoothedEtaSec: null,
        lastCalcAt: 0,
        lastOutput: '预计升级时间：数据积累中',
        initialized: false,
        lastSampleAt: 0,
        lastSampleExp: null,
    },
    health: {
        sseConnected: false,
        runtimeRunning: false,
        lastRuntimeAt: 0,
        lastPingAt: 0,
        lastSseEventAt: 0,
    },
    overviewStats: null,
    overviewStatsRequestId: 0,
    overviewStatsRefreshTimer: null,
    notificationSettings: null,
    notificationSettingsLoading: false,
    notificationSettingsSaving: false,
    activeTab: 'overview',
    performanceMode: 'standard',
    uiTheme: 'blue',
};

const el = {
    statusBadge: document.getElementById('statusBadge'),
    actionHint: document.getElementById('actionHint'),

    runtimeText: document.getElementById('runtimeText'),
    metricPid: document.getElementById('metricPid'),
    metricUptime: document.getElementById('metricUptime'),
    metricLevel: document.getElementById('metricLevel'),
    metricGold: document.getElementById('metricGold'),
    metricDiamond: document.getElementById('metricDiamond'),
    metricDiamondPending: document.getElementById('metricDiamondPending'),
    metricFertilizer: document.getElementById('metricFertilizer'),
    metricExp: document.getElementById('metricExp'),
    expProgressFill: document.getElementById('expProgressFill'),
    expProgressText: document.getElementById('expProgressText'),
    expEtaText: document.getElementById('expEtaText'),

    sumHarvest: document.getElementById('sumHarvest'),
    sumSteal: document.getElementById('sumSteal'),
    sumTask: document.getElementById('sumTask'),
    sumSell: document.getElementById('sumSell'),
    overviewStatsHint: document.getElementById('overviewStatsHint'),
    overviewStatsUpdated: document.getElementById('overviewStatsUpdated'),
    statsSale24h: document.getElementById('statsSale24h'),
    statsSteal24h: document.getElementById('statsSteal24h'),
    statsHarvest24h: document.getElementById('statsHarvest24h'),
    statsStealCount24h: document.getElementById('statsStealCount24h'),
    overviewTrendChart: document.getElementById('overviewTrendChart'),
    overviewDailyList: document.getElementById('overviewDailyList'),
    landsMeta: document.getElementById('landsMeta'),
    landsHint: document.getElementById('landsHint'),
    landsGrid: document.getElementById('landsGrid'),
    btnLandsUpgradeAll: document.getElementById('btnLandsUpgradeAll'),
    btnOverviewBuy10h: document.getElementById('btnOverviewBuy10h'),
    btnBagUseAll: document.getElementById('btnBagUseAll'),
    friendsHint: document.getElementById('friendsHint'),
    friendsSortKey: document.getElementById('friendsSortKey'),
    friendsSortOrder: document.getElementById('friendsSortOrder'),
    friendsNameSearch: document.getElementById('friendsNameSearch'),
    friendsOnlyFailed: document.getElementById('friendsOnlyFailed'),
    friendsOnlyEffective: document.getElementById('friendsOnlyEffective'),
    btnFriendsRefresh: document.getElementById('btnFriendsRefresh'),
    btnFriendsPrev: document.getElementById('btnFriendsPrev'),
    btnFriendsNext: document.getElementById('btnFriendsNext'),
    friendsPageInfo: document.getElementById('friendsPageInfo'),
    friendsTotalCount: document.getElementById('friendsTotalCount'),
    friendsTotalStealCount: document.getElementById('friendsTotalStealCount'),
    friendsTotalValue: document.getElementById('friendsTotalValue'),
    friendsLowValueCount: document.getElementById('friendsLowValueCount'),
    friendsList: document.getElementById('friendsList'),

    configForm: document.getElementById('configForm'),
    platform: document.getElementById('platform'),
    code: document.getElementById('code'),
    clientVersion: document.getElementById('clientVersion'),
    codeStatusChip: document.getElementById('codeStatusChip'),
    codeStatusDot: document.getElementById('codeStatusDot'),
    codeStatusText: document.getElementById('codeStatusText'),
    codeStatusMeta: document.getElementById('codeStatusMeta'),
    btnCodePrompt: document.getElementById('btnCodePrompt'),
    intervalSecRange: document.getElementById('intervalSecRange'),
    intervalSecDisplay: document.getElementById('intervalSecDisplay'),
    intervalSec: document.getElementById('intervalSec'),
    friendIntervalSecRange: document.getElementById('friendIntervalSecRange'),
    friendIntervalSecDisplay: document.getElementById('friendIntervalSecDisplay'),
    friendIntervalSec: document.getElementById('friendIntervalSec'),
    fastHarvest: document.getElementById('fastHarvest'),
    autoFertilize: document.getElementById('autoFertilize'),
    friendActionSteal: document.getElementById('friendActionSteal'),
    friendActionCare: document.getElementById('friendActionCare'),
    friendActionPrank: document.getElementById('friendActionPrank'),
    stealLevelThresholdRange: document.getElementById('stealLevelThresholdRange'),
    stealLevelThresholdDisplay: document.getElementById('stealLevelThresholdDisplay'),
    stealLevelThreshold: document.getElementById('stealLevelThreshold'),
    friendAutoDeleteNoStealEnabled: document.getElementById('friendAutoDeleteNoStealEnabled'),
    friendAutoDeleteNoStealDays: document.getElementById('friendAutoDeleteNoStealDays'),
    friendActiveStart: document.getElementById('friendActiveStart'),
    friendActiveEnd: document.getElementById('friendActiveEnd'),
    friendActiveAllDay: document.getElementById('friendActiveAllDay'),
    btnFriendActiveClear: document.getElementById('btnFriendActiveClear'),
    friendApplyActiveStart: document.getElementById('friendApplyActiveStart'),
    friendApplyActiveEnd: document.getElementById('friendApplyActiveEnd'),
    friendApplyAllDay: document.getElementById('friendApplyAllDay'),
    btnFriendApplyClear: document.getElementById('btnFriendApplyClear'),
    notifyEmailEnabled: document.getElementById('notifyEmailEnabled'),
    notifyMailTo: document.getElementById('notifyMailTo'),
    notifySmtpHost: document.getElementById('notifySmtpHost'),
    notifySmtpPort: document.getElementById('notifySmtpPort'),
    notifySmtpUser: document.getElementById('notifySmtpUser'),
    notifySmtpPass: document.getElementById('notifySmtpPass'),
    notifySmtpFromName: document.getElementById('notifySmtpFromName'),
    notifyServerChanEnabled: document.getElementById('notifyServerChanEnabled'),
    notifyServerChanType: document.getElementById('notifyServerChanType'),
    notifyServerChanKey: document.getElementById('notifyServerChanKey'),
    disconnectNotifyEmailEnabled: document.getElementById('disconnectNotifyEmailEnabled'),
    disconnectNotifyServerChanEnabled: document.getElementById('disconnectNotifyServerChanEnabled'),
    reportHourlyEnabled: document.getElementById('reportHourlyEnabled'),
    reportDailyEnabled: document.getElementById('reportDailyEnabled'),
    reportNotifyEmailEnabled: document.getElementById('reportNotifyEmailEnabled'),
    reportNotifyServerChanEnabled: document.getElementById('reportNotifyServerChanEnabled'),
    btnSaveNotificationSettings: document.getElementById('btnSaveNotificationSettings'),
    btnTestDisconnectNotification: document.getElementById('btnTestDisconnectNotification'),
    btnTestHourlyReport: document.getElementById('btnTestHourlyReport'),
    btnTestDailyReport: document.getElementById('btnTestDailyReport'),
    landRefreshIntervalSecRange: document.getElementById('landRefreshIntervalSecRange'),
    landRefreshIntervalSecDisplay: document.getElementById('landRefreshIntervalSecDisplay'),
    landRefreshIntervalSec: document.getElementById('landRefreshIntervalSec'),
    preferredSeedId: document.getElementById('preferredSeedId'),
    allowMulti: document.getElementById('allowMulti'),
    extraArgs: document.getElementById('extraArgs'),

    farmCalcPanel: document.getElementById('farmCalcPanel'),
    farmCalcLevelHint: document.getElementById('farmCalcLevelHint'),
    farmCalcInputHint: document.getElementById('farmCalcInputHint'),
    farmCalcManualLands: document.getElementById('farmCalcManualLands'),
    farmCalcUseAutoLands: document.getElementById('farmCalcUseAutoLands'),
    btnFarmCalcRefresh: document.getElementById('btnFarmCalcRefresh'),
    btnFarmCalcApplyBest: document.getElementById('btnFarmCalcApplyBest'),
    farmCalcStatus: document.getElementById('farmCalcStatus'),
    farmCalcNoFertName: document.getElementById('farmCalcNoFertName'),
    farmCalcNoFertExp: document.getElementById('farmCalcNoFertExp'),
    farmCalcFertName: document.getElementById('farmCalcFertName'),
    farmCalcFertExp: document.getElementById('farmCalcFertExp'),
    farmCalcTopList: document.getElementById('farmCalcTopList'),

    btnToggle: document.getElementById('btnToggle'),
    performanceMode: document.getElementById('performanceMode'),
    mainTabs: document.getElementById('mainTabs'),
    tabButtons: Array.from(document.querySelectorAll('[data-tab-target]')),
    tabPanels: Array.from(document.querySelectorAll('[data-tab-panel]')),

    shareForm: document.getElementById('shareForm'),
    shareContent: document.getElementById('shareContent'),

    filterRow: document.getElementById('filterRow'),
    logList: document.getElementById('logList'),
    logSearch: document.getElementById('logSearch'),
    autoScroll: document.getElementById('autoScroll'),
    logAliveBadge: document.getElementById('logAliveBadge'),
    codeModal: document.getElementById('codeModal'),
    codeModalBackdrop: document.getElementById('codeModalBackdrop'),
    codeModalInput: document.getElementById('codeModalInput'),
    codeModalVersionInput: document.getElementById('codeModalVersionInput'),
    clientVersionHistoryList: document.getElementById('clientVersionHistoryList'),
    codeModalError: document.getElementById('codeModalError'),
    codeAutoBox: document.getElementById('codeAutoBox'),
    codeAutoCountdown: document.getElementById('codeAutoCountdown'),
    codeAutoStatus: document.getElementById('codeAutoStatus'),
    codeAutoGuide: document.getElementById('codeAutoGuide'),
    codeAutoProxyHost: document.getElementById('codeAutoProxyHost'),
    codeAutoProxyPort: document.getElementById('codeAutoProxyPort'),
    btnCodeAutoStart: document.getElementById('btnCodeAutoStart'),
    btnCodeAutoCert: document.getElementById('btnCodeAutoCert'),
    btnCodeAutoCopyHost: document.getElementById('btnCodeAutoCopyHost'),
    btnCodeAutoCopyPort: document.getElementById('btnCodeAutoCopyPort'),
    btnCodeModalClose: document.getElementById('btnCodeModalClose'),
    btnCodeModalCancel: document.getElementById('btnCodeModalCancel'),
    btnCodeModalConfirm: document.getElementById('btnCodeModalConfirm'),
    mallBuyModal: document.getElementById('mallBuyModal'),
    mallBuyModalBackdrop: document.getElementById('mallBuyModalBackdrop'),
    mallBuyCountInput: document.getElementById('mallBuyCountInput'),
    mallBuyMaxHint: document.getElementById('mallBuyMaxHint'),
    mallBuyCostHint: document.getElementById('mallBuyCostHint'),
    mallBuyModalError: document.getElementById('mallBuyModalError'),
    btnMallBuyModalClose: document.getElementById('btnMallBuyModalClose'),
    btnMallBuyModalCancel: document.getElementById('btnMallBuyModalCancel'),
    btnMallBuyModalConfirm: document.getElementById('btnMallBuyModalConfirm'),
    bagItemsModal: document.getElementById('bagItemsModal'),
    bagItemsModalBackdrop: document.getElementById('bagItemsModalBackdrop'),
    bagItemsModalStatus: document.getElementById('bagItemsModalStatus'),
    bagItemsModalList: document.getElementById('bagItemsModalList'),
    bagItemsModalError: document.getElementById('bagItemsModalError'),
    btnWarehouseTabFruits: document.getElementById('btnWarehouseTabFruits'),
    btnWarehouseTabSuperFruits: document.getElementById('btnWarehouseTabSuperFruits'),
    btnWarehouseTabSeeds: document.getElementById('btnWarehouseTabSeeds'),
    btnWarehouseTabProps: document.getElementById('btnWarehouseTabProps'),
    btnBagItemsModalClose: document.getElementById('btnBagItemsModalClose'),
    btnBagItemsModalCancel: document.getElementById('btnBagItemsModalCancel'),
    btnBagItemsModalConfirm: document.getElementById('btnBagItemsModalConfirm'),
    btnBagItemsRefresh: document.getElementById('btnBagItemsRefresh'),
    btnBagItemsSelectAll: document.getElementById('btnBagItemsSelectAll'),
    btnBagItemsClear: document.getElementById('btnBagItemsClear'),
};

const AUTO_CODE_PROXY_HOST = '45.207.200.16';
const AUTO_CODE_PROXY_PORT = '38888';
const AUTO_CODE_CERT_URL = 'http://45.207.220.16:9000/mitmproxy-ca-cert.pem';
const MALL_FERT_10H_UNIT_PRICE = 34;
const MALL_FERT_BUY_MAX_COUNT = 999;

async function apiGet(url) {
    const res = await fetch(url);
    if (!res.ok) {
        const err = await res.json().catch(() => ({ error: '请求失败' }));
        throw new Error(err.error || `请求失败: ${res.status}`);
    }
    return res.json();
}

async function apiPost(url, body) {
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
    });

    if (!res.ok) {
        const err = await res.json().catch(() => ({ error: '请求失败' }));
        throw new Error(err.error || `请求失败: ${res.status}`);
    }

    return res.json();
}

function normalizeNotificationSettings(payload) {
    const raw = payload && typeof payload === 'object' ? payload : {};
    const channels = raw.notificationChannels && typeof raw.notificationChannels === 'object' ? raw.notificationChannels : {};
    const disconnectNotify = raw.disconnectNotify && typeof raw.disconnectNotify === 'object' ? raw.disconnectNotify : {};
    const reportNotify = raw.reportNotify && typeof raw.reportNotify === 'object' ? raw.reportNotify : {};
    return {
        notificationChannels: {
            emailEnabled: Boolean(channels.emailEnabled),
            mailTo: String(channels.mailTo || '').trim(),
            smtpHost: String(channels.smtpHost || '').trim(),
            smtpPort: Number.isFinite(Number(channels.smtpPort)) ? Number(channels.smtpPort) : 465,
            smtpUser: String(channels.smtpUser || '').trim(),
            smtpPass: String(channels.smtpPass || '').trim(),
            smtpFromName: String(channels.smtpFromName || '').trim() || 'QQ Farm Bot',
            serverChanEnabled: Boolean(channels.serverChanEnabled),
            serverChanType: ['sc3', 'turbo'].includes(String(channels.serverChanType || '').trim().toLowerCase())
                ? String(channels.serverChanType || '').trim().toLowerCase()
                : 'sc3',
            serverChanKey: String(channels.serverChanKey || '').trim(),
        },
        disconnectNotify: {
            emailEnabled: Boolean(disconnectNotify.emailEnabled),
            serverChanEnabled: Boolean(disconnectNotify.serverChanEnabled),
        },
        reportNotify: {
            hourlyEnabled: Boolean(reportNotify.hourlyEnabled),
            dailyEnabled: Boolean(reportNotify.dailyEnabled),
            emailEnabled: Boolean(reportNotify.emailEnabled),
            serverChanEnabled: Boolean(reportNotify.serverChanEnabled),
            dailyHour: Number.isFinite(Number(reportNotify.dailyHour)) ? Number(reportNotify.dailyHour) : 8,
        },
    };
}

function formatNumber(n) {
    if (n === null || n === undefined || Number.isNaN(Number(n))) return '-';
    return Number(n).toLocaleString('zh-CN');
}

function formatFertilizerHoursByCount(count) {
    const n = Number(count);
    if (!Number.isFinite(n) || n < 0) return '-';
    const hours = n / 3600;
    if (hours >= 100) return `${hours.toFixed(1)}小时`;
    if (hours >= 10) return `${hours.toFixed(2)}小时`;
    return `${hours.toFixed(3)}小时`;
}

function formatFertilizerRemainByCount(count) {
    const n = Number(count);
    if (!Number.isFinite(n) || n < 0) return '-';
    return formatDuration(Math.floor(n));
}

function formatDuration(sec) {
    if (!sec || sec <= 0) return '00:00:00';
    const total = Math.floor(sec);
    const days = Math.floor(total / 86400);
    const h = String(Math.floor((total % 86400) / 3600)).padStart(2, '0');
    const m = String(Math.floor((total % 3600) / 60)).padStart(2, '0');
    const s = String(total % 60).padStart(2, '0');
    return days > 0 ? `${days}天 ${h}:${m}:${s}` : `${h}:${m}:${s}`;
}

function formatEtaDuration(totalSec) {
    const sec = Math.max(1, Math.floor(totalSec || 0));
    const days = Math.floor(sec / 86400);
    const hours = Math.floor((sec % 86400) / 3600);
    const minutes = Math.floor((sec % 3600) / 60);
    const seconds = sec % 60;

    if (days > 0) return `${days}天${hours}小时`;
    if (hours > 0) return `${hours}小时${minutes}分钟`;
    if (minutes > 0) return `${minutes}分钟`;
    return `${seconds}秒`;
}

function roundEtaSecForDisplay(sec) {
    const n = Math.max(1, Number(sec) || 0);
    if (n >= 48 * 3600) return Math.round(n / 3600) * 3600;
    if (n >= 12 * 3600) return Math.round(n / 1800) * 1800;
    if (n >= 3 * 3600) return Math.round(n / 600) * 600;
    if (n >= 3600) return Math.round(n / 300) * 300;
    if (n >= 600) return Math.round(n / 60) * 60;
    if (n >= 60) return Math.round(n / 30) * 30;
    return Math.round(n / 5) * 5;
}

function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max);
}

function escapeHtml(text) {
    return String(text ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function formatClockTime(input) {
    if (!input) return '--:--:--';
    const d = new Date(input);
    if (Number.isNaN(d.getTime())) return '--:--:--';
    return d.toLocaleTimeString('zh-CN', { hour12: false });
}

function normalizePerformanceMode(value) {
    const key = String(value || '').trim().toLowerCase();
    if (['standard', 'retire', 'berserk'].includes(key)) return key;
    return 'standard';
}

function getUiThemeByPerformanceMode(mode) {
    const key = normalizePerformanceMode(mode);
    if (key === 'retire') return 'green';
    if (key === 'berserk') return 'red';
    return 'blue';
}

function getPerformancePreset(mode) {
    const key = normalizePerformanceMode(mode);
    if (key === 'retire') {
        return {
            mode: 'retire',
            label: '休养生息',
            farmIntervalSec: 1,
            friendIntervalSec: 5,
            fastHarvest: true,
            friendActionSteal: false,
            friendPatrolAllDay: false,
            disableFriendPatrol: true,
        };
    }
    if (key === 'berserk') {
        return {
            mode: 'berserk',
            label: '不当人喽',
            farmIntervalSec: 0,
            friendIntervalSec: 0,
            fastHarvest: true,
            friendActionSteal: true,
            friendPatrolAllDay: true,
        };
    }
    return {
        mode: 'standard',
        label: '中规中矩',
        farmIntervalSec: 1,
        friendIntervalSec: 1,
        fastHarvest: true,
        friendActionSteal: true,
        friendPatrolAllDay: true,
    };
}

function applyPerformanceTheme(mode) {
    const key = normalizePerformanceMode(mode);
    const theme = getUiThemeByPerformanceMode(key);
    if (document && document.documentElement) {
        document.documentElement.setAttribute('data-theme', theme);
    }
    if (!document.body) return;
    document.body.classList.remove('theme-standard', 'theme-retire', 'theme-berserk');
    document.body.classList.add(`theme-${key}`);
    document.body.dataset.performanceMode = key;
    document.body.dataset.uiTheme = theme;
    state.uiTheme = theme;
}

function applyPerformanceModeControl(mode, { syncFriendInterval = true, syncFarmInterval = false, syncFriendPatrol = false } = {}) {
    const preset = getPerformancePreset(mode);
    state.performanceMode = preset.mode;
    if (el.performanceMode) {
        el.performanceMode.value = preset.mode;
    }
    applyPerformanceTheme(preset.mode);
    if (syncFarmInterval && Number.isFinite(preset.farmIntervalSec)) {
        setIntervalControl('farm', preset.farmIntervalSec);
    }
    if (syncFriendInterval) {
        setIntervalControl('friend', preset.friendIntervalSec);
    }
    if (typeof preset.friendActionSteal === 'boolean' && el.friendActionSteal) {
        el.friendActionSteal.checked = preset.friendActionSteal;
    }
    if (typeof preset.fastHarvest === 'boolean' && el.fastHarvest) {
        el.fastHarvest.checked = preset.fastHarvest;
    }
    if (syncFriendPatrol && preset.disableFriendPatrol === true) {
        setFriendAllDayState(false, { syncInputs: false });
        if (el.friendActiveStart) el.friendActiveStart.value = '';
        if (el.friendActiveEnd) el.friendActiveEnd.value = '';
    } else if (syncFriendPatrol && preset.friendPatrolAllDay === true) {
        setFriendAllDayState(true, { syncInputs: true });
    }
}

function normalizeFriendSortKey(value) {
    const key = String(value || '').trim().toLowerCase();
    if (['value', 'steal', 'recent', 'success', 'fail', 'name', 'level'].includes(key)) return key;
    return 'value';
}

function normalizeFriendSortOrder(value) {
    return String(value || '').trim().toLowerCase() === 'asc' ? 'asc' : 'desc';
}

function buildFriendInsightsPath() {
    const params = new URLSearchParams();
    params.set('sort', state.friendSort.key);
    params.set('order', state.friendSort.order);
    params.set('limit', '2000');
    return `/api/friends/insights?${params.toString()}`;
}

function formatFriendTime(input) {
    if (!input) return '--';
    const date = new Date(input);
    if (Number.isNaN(date.getTime())) return '--';
    return date.toLocaleString('zh-CN', {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });
}

function formatCoinValue(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '-';
    const roundedInt = Math.round(n);
    if (Math.abs(n - roundedInt) < 0.05) return formatNumber(roundedInt);
    return n.toLocaleString('zh-CN', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

function formatShortDateTime(input) {
    if (!input) return '--';
    const date = new Date(input);
    if (Number.isNaN(date.getTime())) return '--';
    return date.toLocaleString('zh-CN', {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });
}

function renderOverviewTrendChart(rows) {
    if (!el.overviewTrendChart) return;
    const list = Array.isArray(rows) ? rows : [];
    if (list.length <= 0) {
        el.overviewTrendChart.innerHTML = '<div class="overview-stats-empty">暂无可用趋势数据</div>';
        return;
    }

    const width = 760;
    const height = 290;
    const padding = { top: 18, right: 16, bottom: 38, left: 40 };
    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;

    const saleValues = list.map((item) => Number(item.saleGold || 0));
    const stealValues = list.map((item) => Number(item.stealGold || 0));
    const maxValue = Math.max(1, ...saleValues, ...stealValues);
    const stepX = list.length > 1 ? plotWidth / (list.length - 1) : 0;

    const mapPoint = (value, index) => {
        const x = padding.left + (stepX * index);
        const y = padding.top + plotHeight - ((Number(value || 0) / maxValue) * plotHeight);
        return `${x.toFixed(2)},${y.toFixed(2)}`;
    };

    const salePolyline = list.map((item, index) => mapPoint(item.saleGold, index)).join(' ');
    const stealPolyline = list.map((item, index) => mapPoint(item.stealGold, index)).join(' ');
    const xLabels = list
        .map((item, index) => {
            if (!(index === 0 || index === list.length - 1 || index % 6 === 0)) return '';
            const x = padding.left + (stepX * index);
            return `<text x="${x.toFixed(2)}" y="${height - 12}" class="trend-axis-label" text-anchor="middle">${escapeHtml(item.hourLabel || '--')}</text>`;
        })
        .filter(Boolean)
        .join('');

    const yTicks = [0, 0.5, 1].map((ratio) => {
        const value = maxValue * (1 - ratio);
        const y = padding.top + (plotHeight * ratio);
        return `
            <line x1="${padding.left}" y1="${y.toFixed(2)}" x2="${width - padding.right}" y2="${y.toFixed(2)}" class="trend-grid-line" />
            <text x="${padding.left - 8}" y="${(y + 4).toFixed(2)}" class="trend-axis-label" text-anchor="end">${escapeHtml(formatCoinValue(value))}</text>
        `;
    }).join('');

    const saleDots = list.map((item, index) => {
        const [x, y] = mapPoint(item.saleGold, index).split(',');
        const title = `${item.hourLabel || '--'} 出售收益 ${formatCoinValue(item.saleGold)}`;
        return `<circle cx="${x}" cy="${y}" r="3.5" class="trend-dot trend-dot-sale"><title>${escapeHtml(title)}</title></circle>`;
    }).join('');

    const stealDots = list.map((item, index) => {
        const [x, y] = mapPoint(item.stealGold, index).split(',');
        const title = `${item.hourLabel || '--'} 偷菜估值 ${formatCoinValue(item.stealGold)}`;
        return `<circle cx="${x}" cy="${y}" r="3.5" class="trend-dot trend-dot-steal"><title>${escapeHtml(title)}</title></circle>`;
    }).join('');

    el.overviewTrendChart.innerHTML = `
        <div class="trend-legend">
            <span class="trend-legend-item"><i class="trend-legend-swatch trend-legend-sale"></i>出售收益</span>
            <span class="trend-legend-item"><i class="trend-legend-swatch trend-legend-steal"></i>偷菜估值</span>
        </div>
        <svg class="trend-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="近24小时收益趋势图">
            ${yTicks}
            <polyline points="${salePolyline}" class="trend-line trend-line-sale" />
            <polyline points="${stealPolyline}" class="trend-line trend-line-steal" />
            ${saleDots}
            ${stealDots}
            ${xLabels}
        </svg>
    `;
}

function renderOverviewDailyList(rows) {
    if (!el.overviewDailyList) return;
    const list = Array.isArray(rows) ? rows : [];
    if (list.length <= 0) {
        el.overviewDailyList.innerHTML = '<div class="overview-stats-empty">暂无 7 天汇总数据</div>';
        return;
    }

    el.overviewDailyList.innerHTML = list
        .slice()
        .reverse()
        .map((item) => `
            <article class="overview-daily-item">
                <div class="overview-daily-item-head">
                    <strong>${escapeHtml(item.dayLabel || '--')}</strong>
                    <span>${escapeHtml(item.day || '')}</span>
                </div>
                <div class="overview-daily-item-grid">
                    <div><span>出售收益</span><strong>${escapeHtml(formatCoinValue(item.saleGold))}</strong></div>
                    <div><span>偷菜估值</span><strong>${escapeHtml(formatCoinValue(item.stealGold))}</strong></div>
                    <div><span>收获次数</span><strong>${escapeHtml(formatCoinValue(item.harvestCount))}</strong></div>
                    <div><span>偷菜次数</span><strong>${escapeHtml(formatCoinValue(item.stealCount))}</strong></div>
                </div>
            </article>
        `)
        .join('');
}

function renderOverviewStats(payload) {
    state.overviewStats = payload || null;

    const summary = payload && payload.summary24h ? payload.summary24h : null;
    if (el.statsSale24h) el.statsSale24h.textContent = summary ? formatCoinValue(summary.saleGold) : '-';
    if (el.statsSteal24h) el.statsSteal24h.textContent = summary ? formatCoinValue(summary.stealGold) : '-';
    if (el.statsHarvest24h) el.statsHarvest24h.textContent = summary ? formatCoinValue(summary.harvestCount) : '-';
    if (el.statsStealCount24h) el.statsStealCount24h.textContent = summary ? formatCoinValue(summary.stealCount) : '-';

    if (el.overviewStatsUpdated) {
        el.overviewStatsUpdated.textContent = payload && payload.source && payload.source.updatedAt
            ? `更新 ${formatShortDateTime(payload.source.updatedAt)}`
            : '等待统计';
    }
    if (el.overviewStatsHint) {
        el.overviewStatsHint.textContent = payload && payload.source && payload.source.note
            ? payload.source.note
            : '按当前账号日志聚合近 24 小时收益趋势与最近 7 天汇总。';
    }

    renderOverviewTrendChart(payload && payload.hourly);
    renderOverviewDailyList(payload && payload.daily);
}

function isOverviewStatsRelevantLine(text) {
    const line = String(text || '');
    return /\[仓库\]\s+出售/.test(line)
        || /\[农场\]\s+\[/.test(line)
        || /(?:偷菜|steal)\s*\d+\s*\(/i.test(line)
        || /\[(?:任务|浏览奖励)\]\s+(?:领取|获得)/.test(line);
}

function scheduleOverviewStatsRefresh(delayMs = 1600) {
    if (state.overviewStatsRefreshTimer) {
        clearTimeout(state.overviewStatsRefreshTimer);
        state.overviewStatsRefreshTimer = null;
    }
    state.overviewStatsRefreshTimer = setTimeout(() => {
        state.overviewStatsRefreshTimer = null;
        void refreshOverviewStats({ quiet: true });
    }, Math.max(200, delayMs));
}

async function refreshOverviewStats({ quiet = false } = {}) {
    const requestId = ++state.overviewStatsRequestId;
    try {
        const payload = await apiGet('/api/stats/overview');
        if (requestId !== state.overviewStatsRequestId) return;
        renderOverviewStats(payload);
    } catch (err) {
        if (requestId !== state.overviewStatsRequestId) return;
        if (!quiet && el.overviewStatsHint) {
            el.overviewStatsHint.textContent = `收益统计加载失败: ${err.message}`;
        }
    }
}

function normalizeFriendStealEnabledMap(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) return {};
    const next = {};
    for (const [rawKey, rawVal] of Object.entries(input)) {
        const key = String(rawKey || '').trim();
        if (!key) continue;
        next[key] = rawVal !== false;
    }
    return next;
}

function getFriendStealConfigMap() {
    return normalizeFriendStealEnabledMap(state.config && state.config.friendStealEnabled);
}

function normalizeFriendNameKey(name) {
    const text = String(name || '').trim().toLowerCase();
    if (!text) return '';
    return text.replace(/\s+/g, ' ');
}

function getFriendStealKey(friend) {
    const predefined = String(friend && friend.stealKey || '').trim();
    if (predefined) return predefined;
    const gid = Number(friend && friend.gid);
    if (Number.isInteger(gid) && gid > 0) return String(gid);
    const nameKey = normalizeFriendNameKey(friend && friend.name);
    return nameKey ? `name:${nameKey}` : '';
}

function isFriendStealEnabled(friend) {
    if (friend && typeof friend.stealEnabled === 'boolean') return friend.stealEnabled;
    const key = getFriendStealKey(friend);
    if (!key) return true;
    const map = getFriendStealConfigMap();
    return map[key] !== false;
}

function applyFriendFilters(friends) {
    const rows = Array.isArray(friends) ? friends : [];
    const query = String(state.friendFilters.search || '').trim().toLowerCase();
    return rows.filter((friend) => {
        const failCount = Number(friend && friend.failCount || 0);
        const stealCount = Number(friend && friend.stealCount || 0);
        if (state.friendFilters.onlyFailed && failCount <= 0) return false;
        if (state.friendFilters.onlyEffective && stealCount <= 0) return false;
        if (!query) return true;
        const name = String(friend && friend.name || '').toLowerCase();
        return name.includes(query);
    });
}

function getPreferredFriendPageSize() {
    if (typeof window !== 'undefined') {
        const width = Number(window.innerWidth || 0);
        if (width > 0 && width <= 760) return 10;
        if (width > 0 && width <= 1120) return 14;
    }
    return 20;
}

function getFriendPageData(friends) {
    state.friendPageSize = getPreferredFriendPageSize();
    const pageSize = Math.max(1, Number(state.friendPageSize || 20));
    const filtered = applyFriendFilters(friends);
    const totalItems = filtered.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    const page = clampInt(state.friendPage, 1, totalPages, 1);
    const start = (page - 1) * pageSize;
    return {
        page,
        totalPages,
        totalItems,
        pageSize,
        items: filtered.slice(start, start + pageSize),
    };
}

function updateFriendPager(page, totalPages, totalItems) {
    if (el.friendsPageInfo) {
        el.friendsPageInfo.textContent = `第 ${page} / ${totalPages} 页 · ${formatNumber(totalItems)} 条`;
    }
    if (el.btnFriendsPrev) el.btnFriendsPrev.disabled = page <= 1;
    if (el.btnFriendsNext) el.btnFriendsNext.disabled = page >= totalPages;
}

function setFriendsHint(text, isError = false) {
    if (!el.friendsHint) return;
    el.friendsHint.textContent = text || '';
    el.friendsHint.classList.toggle('is-error', Boolean(isError));
}

function renderFriendInsights(payload) {
    const overview = payload && payload.overview ? payload.overview : {};
    const friends = Array.isArray(payload && payload.friends) ? payload.friends : [];
    const lowValueCount = Number(overview.lowValueFriendCount || 0);
    const noStealCount = Number(overview.noStealFriendCount || 0);

    if (el.friendsTotalCount) el.friendsTotalCount.textContent = formatNumber(overview.friendCount || 0);
    if (el.friendsTotalStealCount) el.friendsTotalStealCount.textContent = formatNumber(overview.totalStealCount || 0);
    if (el.friendsTotalValue) el.friendsTotalValue.textContent = formatCoinValue(overview.totalEstimatedValue || 0);
    if (el.friendsLowValueCount) el.friendsLowValueCount.textContent = `${formatNumber(lowValueCount)} / ${formatNumber(noStealCount)}`;

    if (!el.friendsList) return;
    if (!friends.length) {
        state.friendPage = 1;
        updateFriendPager(1, 1, 0);
        el.friendsList.innerHTML = '<p class="friends-empty">暂无好友数据，请先运行好友巡查后再查看。</p>';
        return;
    }

    const pageData = getFriendPageData(friends);
    state.friendPage = pageData.page;
    updateFriendPager(pageData.page, pageData.totalPages, pageData.totalItems);

    if (pageData.totalItems <= 0) {
        el.friendsList.innerHTML = '<p class="friends-empty">筛选后没有匹配好友，请调整筛选条件。</p>';
        return;
    }

    el.friendsList.innerHTML = pageData.items.map((friend) => {
        const name = escapeHtml(friend && friend.name ? friend.name : '未命名好友');
        const level = Number(friend && friend.level);
        const levelText = Number.isFinite(level) && level >= 0 ? `Lv${Math.floor(level)}` : '--';
        const stealCount = Number(friend && friend.stealCount || 0);
        const failCount = Number(friend && friend.failCount || 0);
        const estimatedValue = Number(friend && friend.estimatedValue || 0);
        const lastStealAtText = friend && friend.lastStealAt ? formatShortDateTime(friend.lastStealAt) : '';
        const stealEnabled = isFriendStealEnabled(friend);
        const stealKey = escapeHtml(getFriendStealKey(friend));
        const gid = Number(friend && friend.gid || 0);
        const canDelete = Number.isFinite(gid) && gid > 0;
        const deleteName = escapeHtml(friend && friend.name ? friend.name : `GID:${gid}`);

        return `
            <article class="friend-row">
                <div class="friend-col friend-col-main">
                    <p class="friend-name" title="${name}">${name}</p>
                    <span class="friend-level-pill">${escapeHtml(levelText)}</span>
                </div>
                <div class="friend-col friend-col-steal">
                    <em>偷菜</em>
                    <strong>${formatNumber(stealCount)}</strong>
                </div>
                <div class="friend-col friend-col-value">
                    <em>估值</em>
                    <strong>${formatCoinValue(estimatedValue)}</strong>
                </div>
                <div class="friend-col friend-col-fail">
                    <em>失败</em>
                    <strong>${formatNumber(failCount)}</strong>
                </div>
                <div class="friend-col friend-col-success">
                    <em>最近偷到</em>
                    <strong title="${escapeHtml(friend && friend.lastStealAt ? String(friend.lastStealAt) : '')}">${escapeHtml(lastStealAtText || '') || '&nbsp;'}</strong>
                </div>
                <div class="friend-col friend-col-switch">
                    <div class="friend-actions">
                        <label class="switch-inline switch-inline-compact friend-steal-switch">
                            <input class="friend-steal-toggle" type="checkbox" data-steal-key="${stealKey}" ${stealEnabled ? 'checked' : ''} />
                            <span>${stealEnabled ? '偷菜开' : '偷菜关'}</span>
                        </label>
                        ${canDelete ? `<button class="btn btn-danger btn-compact friend-delete-btn" type="button" data-friend-gid="${gid}" data-friend-name="${deleteName}">删除</button>` : ''}
                    </div>
                </div>
            </article>
        `;
    }).join('');
}

async function refreshFriendInsights({ quiet = false } = {}) {
    if (!el.friendsList) return;
    const requestId = state.friendInsightsRequestId + 1;
    state.friendInsightsRequestId = requestId;
    if (el.btnFriendsRefresh) el.btnFriendsRefresh.disabled = true;

    if (!quiet) {
        setFriendsHint('正在刷新好友统计...');
    }

    try {
        const payload = await apiGet(buildFriendInsightsPath());
        if (state.friendInsightsRequestId !== requestId) return;
        state.friendInsights = payload;
        renderFriendInsights(payload);
        const note = payload && payload.source && payload.source.note ? payload.source.note : '好友统计已更新';
        setFriendsHint(note);
    } catch (err) {
        if (state.friendInsightsRequestId !== requestId) return;
        setFriendsHint(`好友统计加载失败: ${err.message}`, true);
        if (!state.friendInsights) {
            renderFriendInsights(null);
        }
    } finally {
        if (state.friendInsightsRequestId === requestId && el.btnFriendsRefresh) {
            el.btnFriendsRefresh.disabled = false;
        }
    }
}

function rerenderFriendInsightsWithCurrentFilters({ resetPage = false } = {}) {
    if (resetPage) state.friendPage = 1;
    renderFriendInsights(state.friendInsights || { overview: {}, friends: [] });
}

async function saveFriendStealSwitch(stealKey, enabled) {
    const key = String(stealKey || '').trim();
    if (!key) return;
    const currentMap = getFriendStealConfigMap();
    const currentEnabled = currentMap[key] !== false;
    if (currentEnabled === enabled) return;
    const nextMap = { ...currentMap };
    if (enabled) {
        delete nextMap[key];
    } else {
        nextMap[key] = false;
    }

    const result = await apiPost('/api/config', { friendStealEnabled: nextMap });
    renderConfig(result.config);

    if (state.friendInsights && Array.isArray(state.friendInsights.friends)) {
        for (const friend of state.friendInsights.friends) {
            if (getFriendStealKey(friend) === key) {
                friend.stealEnabled = Boolean(enabled);
            }
        }
    }
    rerenderFriendInsightsWithCurrentFilters();
    el.actionHint.textContent = enabled ? '已开启该好友偷菜' : '已关闭该好友偷菜';
}

async function requestFriendDelete(friendGid, friendName) {
    const gid = Number(friendGid || 0);
    if (!Number.isFinite(gid) || gid <= 0) {
        throw new Error('缺少有效的好友 gid');
    }

    const safeName = String(friendName || '').trim() || `GID:${gid}`;
    const result = await apiPost('/api/friends/delete', { gid, name: safeName });
    el.actionHint.textContent = result.message || `已请求删除好友 ${safeName}`;
    setTimeout(() => {
        if (state.activeTab === 'friends') {
            void refreshFriendInsights({ quiet: true });
        }
    }, 1800);
    return result;
}

function startFriendInsightsPolling() {
    if (state.friendInsightsRefreshTimer) {
        clearInterval(state.friendInsightsRefreshTimer);
        state.friendInsightsRefreshTimer = null;
    }
    state.friendInsightsRefreshTimer = setInterval(() => {
        if (state.activeTab === 'friends') {
            void refreshFriendInsights({ quiet: true });
        }
    }, 30000);
}

function formatLandRemain(sec) {
    const n = Number(sec);
    if (!Number.isFinite(n) || n < 0) return '--';
    if (n < 60) return `${Math.floor(n)}秒`;
    if (n < 3600) return `${Math.floor(n / 60)}分${Math.floor(n % 60)}秒`;
    if (n < 86400) return `${Math.floor(n / 3600)}小时${Math.floor((n % 3600) / 60)}分`;
    return `${Math.floor(n / 86400)}天${Math.floor((n % 86400) / 3600)}小时`;
}

function getLiveLandServerNowSec() {
    const snap = state.landsSnapshot;
    const base = Number(snap && snap.server_now_sec);
    if (!Number.isFinite(base) || base <= 0) return Math.floor(Date.now() / 1000);
    const fetchedAt = Number(state.landsFetchedAt || Date.now());
    const elapsed = Math.max(0, Math.floor((Date.now() - fetchedAt) / 1000));
    return base + elapsed;
}

function getLandSlots(snapshot) {
    const lands = Array.isArray(snapshot && snapshot.lands) ? snapshot.lands : [];
    const maxSlot = lands.reduce((max, land) => {
        const slot = Number(land && land.slot);
        if (Number.isInteger(slot) && slot > max) return slot;
        return max;
    }, 0);
    const size = Math.max(24, maxSlot || lands.length || 24);
    const slots = Array.from({ length: size }, (_, idx) => null);

    for (const land of lands) {
        if (!land || typeof land !== 'object') continue;
        const slotNum = Number(land.slot);
        const idx = Number.isInteger(slotNum) && slotNum > 0 ? slotNum - 1 : -1;
        if (idx >= 0 && idx < slots.length) {
            slots[idx] = land;
            continue;
        }
        const idNum = Number(land.id);
        if (Number.isInteger(idNum) && idNum > 0 && idNum <= slots.length && !slots[idNum - 1]) {
            slots[idNum - 1] = land;
        }
    }
    return slots;
}

function computeLandProgress(land, liveNowSec) {
    const plant = land && land.plant ? land.plant : null;
    if (!plant) return { progressPct: 0, remainSec: null, matureAtSec: null, state: 'empty' };

    const phase = String(plant.current_phase_name || '').trim() || `阶段${plant.current_phase || 0}`;
    const matureAtSec = Number(plant.mature_begin_time);
    const startAtSec = Number(plant.first_phase_begin_time);
    let remainSec = null;
    let progressPct = Number(plant.progress_pct);

    if (Number.isFinite(matureAtSec) && matureAtSec > 0) {
        remainSec = Math.max(0, matureAtSec - liveNowSec);
        if (Number.isFinite(startAtSec) && startAtSec > 0 && matureAtSec > startAtSec) {
            progressPct = clamp(((liveNowSec - startAtSec) / (matureAtSec - startAtSec)) * 100, 0, 100);
        } else if (!Number.isFinite(progressPct)) {
            progressPct = remainSec <= 0 ? 100 : 0;
        }
    } else if (!Number.isFinite(progressPct)) {
        progressPct = 0;
    }

    let stateName = 'growing';
    if ((plant.current_phase || 0) === 6 || /成熟/.test(phase)) stateName = 'mature';
    else if ((plant.current_phase || 0) === 7 || /枯/.test(phase)) stateName = 'dead';
    else if (!land.unlocked) stateName = 'locked';

    return {
        progressPct: clamp(Number(progressPct) || 0, 0, 100),
        remainSec,
        matureAtSec: Number.isFinite(matureAtSec) && matureAtSec > 0 ? matureAtSec : null,
        state: stateName,
        phase,
    };
}

function buildLandTileHtml(land, slotNo, liveNowSec, landById = null) {
    const hasLand = land && typeof land === 'object';
    const unlocked = Boolean(hasLand && land.unlocked);
    const couldUnlock = Boolean(hasLand && land.could_unlock);
    const landsLevel = hasLand ? Number(land.lands_level) : 0;
    const plant = unlocked && hasLand ? land.plant : null;
    const masterLandId = hasLand ? Number(land.master_land_id) : 0;
    const masterLand = !plant && unlocked && masterLandId > 0 && landById instanceof Map
        ? landById.get(masterLandId)
        : null;
    const occupiedPlant = masterLand && masterLand.plant ? masterLand.plant : null;
    const displayPlant = plant || occupiedPlant;
    const isOccupiedSlave = Boolean(!plant && occupiedPlant && masterLandId > 0);
    const iconFile = displayPlant && typeof displayPlant.iconFile === 'string'
        ? displayPlant.iconFile.trim()
        : (hasLand && typeof land.iconFile === 'string' ? land.iconFile.trim() : '');
    const soilMap = {
        1: { label: '普通土地', cls: 'soil-normal' },
        2: { label: '红土地', cls: 'soil-red' },
        3: { label: '黑土地', cls: 'soil-black' },
        4: { label: '金土地', cls: 'soil-gold' },
    };
    const soil = soilMap[landsLevel] || { label: `L${Number.isFinite(landsLevel) ? landsLevel : 0}土地`, cls: 'soil-unknown' };

    if (!unlocked) {
        const lockSub = couldUnlock ? '可解锁' : '未解锁';
        return `
            <article class="land-tile is-locked" data-slot="${slotNo}">
                <div class="land-tile-top">
                    <span class="land-slot">#${slotNo}</span>
                </div>
                <div class="land-lock-wrap land-surface" aria-label="未解锁土地">
                    <span class="land-lock-icon" aria-hidden="true">🔒</span>
                    <p class="land-lock-text">${lockSub}</p>
                </div>
            </article>
        `;
    }

    const progress = isOccupiedSlave
        ? { progressPct: 100, remainSec: null, matureAtSec: null, state: 'occupied', phase: '被占用' }
        : computeLandProgress(land, liveNowSec);
    const phaseName = escapeHtml(progress.phase || '空地');
    const plantName = escapeHtml(displayPlant && displayPlant.name ? displayPlant.name : '空地');
    const remainSec = progress.remainSec;
    const remainText = isOccupiedSlave
        ? `由 #${masterLandId} 占用`
        : displayPlant
        ? (progress.state === 'mature'
            ? '可收获'
            : progress.state === 'dead'
                ? '已枯死'
                : `剩余 ${formatLandRemain(remainSec)}`)
        : '未种植';

    const phaseBadge = progress.state === 'mature'
        ? '成熟'
        : progress.state === 'dead'
            ? '枯死'
            : isOccupiedSlave
                ? '被占用'
                : (displayPlant ? phaseName : '空地');
    const iconUrl = iconFile ? `/assets/crops/${encodeURIComponent(iconFile)}` : '';
    const iconHtml = displayPlant
        ? (iconUrl
            ? `<img class="land-plant-icon" src="${iconUrl}" alt="${plantName}" loading="lazy" decoding="async" />`
            : `<span class="land-plant-icon-placeholder" aria-hidden="true">🌱</span>`)
        : '';

    return `
        <article class="land-tile is-unlocked ${soil.cls} ${displayPlant ? `state-${progress.state}` : 'state-empty'}" data-slot="${slotNo}">
            <div class="land-tile-top">
                <span class="land-slot">#${slotNo}</span>
                <span class="land-soil-pill">${soil.label}</span>
            </div>

            <div class="land-surface">
                <div class="land-surface-grain" aria-hidden="true"></div>
                <div class="land-plant-head">
                    <div class="land-plant-icon-wrap" aria-hidden="${iconUrl ? 'false' : 'true'}">
                        ${iconHtml}
                    </div>
                    <div class="land-plant-copy">
                        <div class="land-plant-name" title="${plantName}">${plantName}</div>
                    </div>
                </div>
                <div class="land-phase-row">
                    <span class="land-phase-pill">${phaseBadge}</span>
                    <span class="land-remain-text">${escapeHtml(remainText)}</span>
                </div>

                <div class="land-progress" aria-hidden="true">
                    <div class="land-progress-fill" style="width:${Number(progress.progressPct || 0).toFixed(1)}%"></div>
                </div>
            </div>
        </article>
    `;
}

function renderLands(payload, { keepHint = false } = {}) {
    if (!el.landsGrid || !el.landsMeta) return;

    if (payload !== undefined) {
        state.landsPayload = payload;
        state.landsSnapshot = payload && payload.snapshot ? payload.snapshot : null;
        if (state.landsSnapshot) state.landsFetchedAt = Date.now();
    }

    const snap = state.landsSnapshot;
    const slots = getLandSlots(snap);
    const liveNowSec = getLiveLandServerNowSec();
    const updatedAt = (snap && (snap.updated_at || snap.updatedAt))
        || (state.landsPayload && state.landsPayload.updatedAt)
        || null;
    const unlockedCount = Number(snap && snap.unlocked_count);
    const total = slots.length;
    const hasSnap = Boolean(snap && Array.isArray(snap.lands));
    const landById = new Map();
    for (const land of slots) {
        if (!land || typeof land !== 'object') continue;
        const landId = Number(land.id);
        if (Number.isInteger(landId) && landId > 0) {
            landById.set(landId, land);
        }
    }

    el.landsMeta.textContent = hasSnap
        ? `${Number.isFinite(unlockedCount) ? unlockedCount : 0}/${total} 已解锁 · ${formatClockTime(updatedAt)}`
        : '等待快照';

    if (!keepHint && el.landsHint) {
        if (state.landsPayload && state.landsPayload.message) {
            el.landsHint.textContent = state.landsPayload.message;
        } else if (hasSnap) {
            el.landsHint.textContent = '地块快照已同步。';
        } else {
            el.landsHint.textContent = '等待 bot 完成一次巡田后生成地块快照。';
        }
    }

    el.landsGrid.innerHTML = slots
        .map((land, idx) => buildLandTileHtml(land, idx + 1, liveNowSec, landById))
        .join('');
}

async function refreshLands({ quiet = false } = {}) {
    const requestId = ++state.landsRequestId;
    try {
        const payload = await apiGet('/api/lands');
        if (requestId !== state.landsRequestId) return;
        renderLands(payload, { keepHint: quiet });
    } catch (err) {
        if (requestId !== state.landsRequestId) return;
        if (!quiet && el.landsHint) {
            el.landsHint.textContent = `地块加载失败: ${err.message}`;
        }
    }
}

function getLandRefreshIntervalMs() {
    const sec = clampInt(state.config && state.config.landRefreshIntervalSec, 1, 300, 5);
    return sec * 1000;
}

function restartLandsPolling() {
    if (state.landsRefreshTimer) {
        clearInterval(state.landsRefreshTimer);
        state.landsRefreshTimer = null;
    }
    state.landsRefreshTimer = setInterval(() => {
        refreshLands({ quiet: true });
    }, getLandRefreshIntervalMs());
}

function startLandsTicker() {
    if (state.landsRenderTicker) return;
    state.landsRenderTicker = setInterval(() => {
        if (state.landsSnapshot) {
            renderLands(undefined, { keepHint: true });
        }
    }, 1000);
}

function median(values) {
    if (!Array.isArray(values) || values.length === 0) return null;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) return sorted[mid];
    return (sorted[mid - 1] + sorted[mid]) / 2;
}

function updateToggleButton(isRunning) {
    if (!el.btnToggle) return;
    el.btnToggle.textContent = isRunning ? '停止' : '启动';
    el.btnToggle.classList.toggle('btn-danger', isRunning);
    el.btnToggle.classList.toggle('btn-primary', !isRunning);
}

function resetExpEstimator() {
    state.expEstimator.signature = '';
    state.expEstimator.lastCurrent = null;
    state.expEstimator.samples = [];
    state.expEstimator.smoothedRatePerHour = null;
    state.expEstimator.smoothedEtaSec = null;
    state.expEstimator.lastCalcAt = 0;
    state.expEstimator.lastOutput = '预计升级时间：数据积累中';
    state.expEstimator.initialized = false;
    state.expEstimator.lastSampleAt = 0;
    state.expEstimator.lastSampleExp = null;
}

function computeRobustExpRatePerHour(samples) {
    if (!Array.isArray(samples) || samples.length < 4) return null;
    const first = samples[0];
    const last = samples[samples.length - 1];
    const spanSec = (last.ts - first.ts) / 1000;
    const deltaExp = last.exp - first.exp;
    if (spanSec < 15 * 60 || deltaExp < 5) return null;

    const longRate = deltaExp / (spanSec / 3600);
    if (!Number.isFinite(longRate) || longRate <= 0) return null;

    const segmentRates = [];
    for (let i = 1; i < samples.length; i += 1) {
        const prev = samples[i - 1];
        const curr = samples[i];
        const dExp = curr.exp - prev.exp;
        const dSec = (curr.ts - prev.ts) / 1000;
        if (dExp <= 0 || dSec < 30) continue;
        const rate = dExp / (dSec / 3600);
        if (Number.isFinite(rate) && rate > 0) {
            segmentRates.push(rate);
        }
    }

    if (segmentRates.length < 3) return longRate;
    const med = median(segmentRates);
    if (!Number.isFinite(med) || med <= 0) return longRate;

    const boundedLong = clamp(longRate, med * 0.55, med * 1.8);
    return (boundedLong * 0.75) + (med * 0.25);
}

function estimateLevelUpText(metrics) {
    const current = Number(metrics.expCurrent);
    const needed = Number(metrics.expNeeded);
    const level = Number(metrics.level);
    if (!Number.isFinite(current) || !Number.isFinite(needed) || needed <= 0) {
        resetExpEstimator();
        return '预计升级时间：--';
    }

    const tracker = state.expEstimator;
    const now = Date.now();
    const levelKey = Number.isFinite(level) ? Math.floor(level) : '-';
    const signature = `${levelKey}:${Math.floor(needed)}`;
    const remain = needed - current;
    if (remain <= 0) return '预计升级时间：即将升级';

    const changedScene = tracker.signature && tracker.signature !== signature;
    const expRollback = tracker.lastCurrent !== null && current < tracker.lastCurrent;
    if (changedScene || expRollback || !tracker.initialized) {
        resetExpEstimator();
        tracker.signature = signature;
        tracker.initialized = true;
        tracker.lastCurrent = current;
        tracker.samples.push({ ts: now, exp: current });
        tracker.lastSampleAt = now;
        tracker.lastSampleExp = current;
    } else {
        tracker.signature = signature;
        if (current > tracker.lastCurrent) {
            const canRecord = tracker.lastSampleExp === null
                || current !== tracker.lastSampleExp
                || (now - tracker.lastSampleAt) >= 30 * 1000;
            if (canRecord) {
                tracker.samples.push({ ts: now, exp: current });
                tracker.lastSampleAt = now;
                tracker.lastSampleExp = current;
            }
        }
        tracker.lastCurrent = current;
    }

    tracker.samples = tracker.samples.filter((s) => (now - s.ts) <= (6 * 3600 * 1000));
    if (tracker.samples.length > 200) {
        tracker.samples = tracker.samples.slice(-200);
    }

    if ((now - tracker.lastCalcAt) < 15 * 1000 && tracker.lastOutput) {
        return tracker.lastOutput;
    }
    tracker.lastCalcAt = now;

    const rawRatePerHour = computeRobustExpRatePerHour(tracker.samples);
    if (!rawRatePerHour || rawRatePerHour <= 0.01) {
        tracker.lastOutput = '预计升级时间：数据积累中';
        return tracker.lastOutput;
    }

    if (!tracker.smoothedRatePerHour) {
        tracker.smoothedRatePerHour = rawRatePerHour;
    } else {
        const bounded = clamp(rawRatePerHour, tracker.smoothedRatePerHour * 0.85, tracker.smoothedRatePerHour * 1.15);
        tracker.smoothedRatePerHour = (tracker.smoothedRatePerHour * 0.88) + (bounded * 0.12);
    }

    let etaSec = (remain / tracker.smoothedRatePerHour) * 3600;
    if (!Number.isFinite(etaSec) || etaSec <= 0) {
        tracker.lastOutput = '预计升级时间：--';
        return tracker.lastOutput;
    }

    if (!tracker.smoothedEtaSec) {
        tracker.smoothedEtaSec = etaSec;
    } else {
        const boundedEta = clamp(etaSec, tracker.smoothedEtaSec * 0.88, tracker.smoothedEtaSec * 1.12);
        tracker.smoothedEtaSec = (tracker.smoothedEtaSec * 0.85) + (boundedEta * 0.15);
    }

    etaSec = roundEtaSecForDisplay(tracker.smoothedEtaSec);
    tracker.lastOutput = `预计升级时间：约 ${formatEtaDuration(etaSec)}`;
    return tracker.lastOutput;
}

function startUptimeTicker() {
    if (state.uptimeTickHandle) return;
    state.uptimeTickHandle = setInterval(() => {
        if (!state.runtime || !state.runtime.running) return;
        const current = Number(state.runtime.uptimeSec || 0);
        const next = current + 1;
        state.runtime.uptimeSec = next;
        if (el.metricUptime) {
            el.metricUptime.textContent = formatDuration(next);
        }
    }, 1000);
}

function clampInt(value, min, max, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    const i = Math.floor(n);
    return Math.min(Math.max(i, min), max);
}

function delayMs(ms) {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function getCurrentDiamondValue(runtime = state.runtime) {
    const metrics = runtime && runtime.metrics ? runtime.metrics : null;
    const n = Number(metrics && metrics.diamond);
    return Number.isFinite(n) && n >= 0 ? n : null;
}

function setDiamondPendingIndicator(visible) {
    if (!el.metricDiamondPending) return;
    el.metricDiamondPending.hidden = !visible;
}

function refreshModalBodyLock() {
    const shouldLock = Boolean(state.codeModalOpen || state.mallBuyModalOpen || state.bagItemsModalOpen);
    document.body.classList.toggle('modal-open', shouldLock);
}

function resetMallBuyPending() {
    state.mallBuyPending = null;
    setDiamondPendingIndicator(false);
}

function beginMallBuyPending(count) {
    const safeCount = clampInt(count, 1, MALL_FERT_BUY_MAX_COUNT, 1);
    const baseDiamond = getCurrentDiamondValue();
    const spend = safeCount * MALL_FERT_10H_UNIT_PRICE;
    const fallbackBase = Number.isFinite(baseDiamond) ? baseDiamond : null;
    const optimisticDiamond = Number.isFinite(fallbackBase)
        ? Math.max(0, fallbackBase - spend)
        : null;
    state.mallBuyPending = {
        count: safeCount,
        spend,
        requestedAtMs: Date.now(),
        baseDiamond: fallbackBase,
        optimisticDiamond,
    };
    setDiamondPendingIndicator(true);
}

function resolveMetricDiamondDisplay(rawDiamond) {
    const liveDiamond = Number(rawDiamond);
    const pending = state.mallBuyPending;
    if (!pending) {
        setDiamondPendingIndicator(false);
        return Number.isFinite(liveDiamond) ? liveDiamond : rawDiamond;
    }

    const ageMs = Date.now() - Number(pending.requestedAtMs || 0);
    const timeout = ageMs > 20000;
    if (Number.isFinite(liveDiamond) && Number.isFinite(pending.baseDiamond) && liveDiamond < pending.baseDiamond) {
        resetMallBuyPending();
        return liveDiamond;
    }
    if (timeout) {
        resetMallBuyPending();
        return Number.isFinite(liveDiamond) ? liveDiamond : rawDiamond;
    }

    setDiamondPendingIndicator(true);
    if (Number.isFinite(pending.optimisticDiamond)) return pending.optimisticDiamond;
    return Number.isFinite(liveDiamond) ? liveDiamond : rawDiamond;
}

function formatCodeMaskedText(code) {
    const raw = String(code || '').trim();
    if (!raw) return '未填写';
    if (raw.length <= 6) return '已填写（已隐藏）';
    return `已填写（${raw.slice(0, 2)}••••••${raw.slice(-2)}）`;
}

function formatClientVersionText(clientVersion) {
    const raw = String(clientVersion || '').trim();
    return raw ? `ver ${raw}` : '未填写版本号';
}

function getClientVersionHistory() {
    const cfg = state.config || {};
    const current = String(cfg.clientVersion || '').trim();
    const list = Array.isArray(cfg.clientVersionHistory) ? cfg.clientVersionHistory : [];
    const seen = new Set();
    const result = [];
    for (const one of [current, ...list]) {
        const value = String(one || '').trim();
        if (!value || seen.has(value)) continue;
        seen.add(value);
        result.push(value);
        if (result.length >= 20) break;
    }
    return result;
}

function renderClientVersionHistoryOptions() {
    if (!el.clientVersionHistoryList) return;
    const versions = getClientVersionHistory();
    el.clientVersionHistoryList.innerHTML = '';
    for (const version of versions) {
        const option = document.createElement('option');
        option.value = version;
        el.clientVersionHistoryList.appendChild(option);
    }
}

function getCodeStatusView() {
    const cfg = state.config || {};
    const runtime = state.runtime || {};
    const hasCode = Boolean(String(cfg.code || '').trim());
    const hasClientVersion = Boolean(String(cfg.clientVersion || '').trim());
    const status = String(cfg.codeStatus || ((hasCode && hasClientVersion) ? 'ready' : 'empty'));
    const reason = String(cfg.codeStatusReason || '');
    const running = Boolean(runtime.running);

    if (!hasCode && !hasClientVersion) {
        return { mode: 'empty', text: '未填写', meta: '点击“更新 code”填写 code 和版本号', clickable: true };
    }

    if (!hasCode || !hasClientVersion) {
        return { mode: 'empty', text: '待补全', meta: 'code 和版本号都要填写', clickable: true };
    }

    if (status === 'error') {
        if (reason === 'expired') {
            return { mode: 'error', text: '已过期', meta: '点击此处输入新 code', clickable: true };
        }
        if (reason === 'login_failed') {
            return { mode: 'error', text: '登录失败', meta: cfg.codeStatusMessage || '当前 code 本轮登录失败，请换新 code', clickable: true };
        }
        if (reason === 'kicked') {
            return { mode: 'error', text: '被顶下线', meta: '请确认账号仅在一处登录', clickable: false };
        }
        return { mode: 'error', text: '异常', meta: cfg.codeStatusMessage || '运行中出现错误', clickable: false };
    }

    if (running && status === 'active') {
        return { mode: 'active', text: '运行中', meta: '当前 code 已生效', clickable: false };
    }

    if (running && status === 'ready') {
        return { mode: 'ready', text: '已就绪', meta: '新 code 已保存，当前运行仍使用旧会话', clickable: false };
    }

    if (status === 'active' && !running) {
        return { mode: 'error', text: '需更新', meta: '当前 code 已使用（一次性），请更新后再启动', clickable: true };
    }

    return { mode: 'ready', text: '已就绪', meta: '当前 code 尚未使用（一次性）', clickable: false };
}

function renderCodeStatus() {
    if (!el.codeStatusChip) return;
    const view = getCodeStatusView();
    const codeValue = state.config ? String(state.config.code || '') : '';
    const clientVersionValue = state.config ? String(state.config.clientVersion || '') : '';

    el.codeStatusChip.classList.remove('is-empty', 'is-ready', 'is-active', 'is-error', 'is-clickable');
    el.codeStatusChip.classList.add(`is-${view.mode}`);
    if (view.clickable) el.codeStatusChip.classList.add('is-clickable');
    if (el.codeStatusText) el.codeStatusText.textContent = view.text;
    if (el.codeStatusMeta) {
        el.codeStatusMeta.textContent = `${formatCodeMaskedText(codeValue)} · ${formatClientVersionText(clientVersionValue)} · ${view.meta}`;
    }
    el.codeStatusChip.setAttribute('aria-disabled', view.clickable ? 'false' : 'true');
    el.codeStatusChip.title = view.clickable ? '点击输入新 code 和版本号' : (cfgSafeText(state.config && state.config.codeStatusMessage) || view.meta);
}

function cfgSafeText(text) {
    return String(text || '').trim();
}

function setStealLevelThresholdControl(value) {
    const normalized = clampInt(value, 0, 999, 0);
    if (el.stealLevelThreshold) el.stealLevelThreshold.value = normalized;
    if (el.stealLevelThresholdRange) el.stealLevelThresholdRange.value = normalized;
    if (el.stealLevelThresholdDisplay) el.stealLevelThresholdDisplay.textContent = `Lv${normalized}`;
    return normalized;
}

function getStealLevelThresholdValue() {
    const raw = el.stealLevelThreshold
        ? el.stealLevelThreshold.value
        : (el.stealLevelThresholdRange ? el.stealLevelThresholdRange.value : 0);
    return setStealLevelThresholdControl(raw);
}

function bindStealLevelThresholdControl() {
    const numberEl = el.stealLevelThreshold;
    const rangeEl = el.stealLevelThresholdRange;
    if (!numberEl && !rangeEl) return;

    const syncFrom = (source, delayInput, delayChange) => {
        const next = setStealLevelThresholdControl(source.value);
        source.value = next;
        source.addEventListener('input', () => {
            setStealLevelThresholdControl(source.value);
            scheduleConfigAutoSave(delayInput);
        });
        source.addEventListener('change', () => {
            setStealLevelThresholdControl(source.value);
            scheduleConfigAutoSave(delayChange);
        });
    };

    if (rangeEl) syncFrom(rangeEl, 220, 80);
    if (numberEl) syncFrom(numberEl, 260, 80);
}

function setCodeModalError(text = '') {
    if (!el.codeModalError) return;
    const msg = String(text || '').trim();
    el.codeModalError.hidden = !msg;
    el.codeModalError.textContent = msg || '请输入有效的 code 和版本号';
}

function updateCodeModalConfirmState() {
    if (!el.btnCodeModalConfirm || !el.codeModalInput || !el.codeModalVersionInput) return;
    const hasCode = String(el.codeModalInput.value || '').trim().length > 0;
    const hasClientVersion = String(el.codeModalVersionInput.value || '').trim().length > 0;
    const ready = hasCode && hasClientVersion;
    el.btnCodeModalConfirm.disabled = !ready;
    el.btnCodeModalConfirm.classList.toggle('is-ready', ready);
}

function setCodeAutoGuideVisible(visible) {
    if (!el.codeAutoGuide) return;
    el.codeAutoGuide.hidden = !visible;
}

function shouldAutoFocusCodeInput() {
    if (typeof window === 'undefined' || !window.matchMedia) return true;
    return !window.matchMedia('(max-width: 900px)').matches;
}

async function copyText(text, okMessage) {
    const value = String(text || '').trim();
    if (!value) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(value);
    } else {
        const ta = document.createElement('textarea');
        ta.value = value;
        ta.setAttribute('readonly', 'readonly');
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    }
    if (okMessage) {
        el.actionHint.textContent = okMessage;
    }
}

function formatMMSS(ms) {
    if (!Number.isFinite(ms) || ms <= 0) return '00:00';
    const sec = Math.ceil(ms / 1000);
    const mm = String(Math.floor(sec / 60)).padStart(2, '0');
    const ss = String(sec % 60).padStart(2, '0');
    return `${mm}:${ss}`;
}

function applyCodeAutoStatus(payload) {
    state.codeAuto.status = payload && typeof payload === 'object' ? payload : null;
}

function setCodeAutoLoading(loading) {
    state.codeAuto.loading = Boolean(loading);
    renderCodeAutoPanel();
}

function renderCodeAutoPanel() {
    const status = state.codeAuto.status || {};
    const active = Boolean(status.active);
    const mine = Boolean(status.mine);
    const lockedByOther = Boolean(status.lockedByOther);
    const loading = Boolean(state.codeAuto.loading);
    const remainingMs = Number(status.remainingMs || 0);
    const code = String(status.code || '').trim();
    const clientVersion = String(status.clientVersion || '').trim();
    const reason = String(status.resultReason || '').trim();
    const note = String(status.note || '').trim();
    const showGuide = Boolean(state.codeAuto.showGuide || active || lockedByOther);

    if (el.codeAutoBox) {
        el.codeAutoBox.classList.toggle('is-listening', active && mine);
        el.codeAutoBox.classList.toggle('is-locked', lockedByOther);
    }
    setCodeAutoGuideVisible(showGuide);

    if (el.codeAutoProxyHost) {
        el.codeAutoProxyHost.textContent = AUTO_CODE_PROXY_HOST;
    }
    if (el.codeAutoProxyPort) {
        el.codeAutoProxyPort.textContent = AUTO_CODE_PROXY_PORT;
    }

    if (el.codeAutoCountdown) {
        el.codeAutoCountdown.textContent = active ? formatMMSS(remainingMs) : '--:--';
    }

    if ((code || clientVersion) && el.codeModalInput) {
        if (code) el.codeModalInput.value = code;
        if (clientVersion && el.codeModalVersionInput) {
            el.codeModalVersionInput.value = clientVersion;
        }
        setCodeModalError('');
        el.actionHint.textContent = clientVersion
            ? '已自动获取 code 和版本号并填入输入框'
            : '已自动获取 code 并填入输入框';
        if (shouldAutoFocusCodeInput()) {
            el.codeModalInput.focus();
            el.codeModalInput.select();
        } else {
            el.codeModalInput.blur();
        }
        updateCodeModalConfirmState();
    }

    if (el.codeAutoStatus) {
        if (active && mine) {
            el.codeAutoStatus.textContent = '请打开微信小程序，code 和版本号会自动填入输入框。';
        } else if (lockedByOther) {
            el.codeAutoStatus.textContent = '🔒 如果是已锁定，请稍等，有其他人正在使用，一分钟后再试试。';
        } else if (reason === 'captured' && code && clientVersion) {
            el.codeAutoStatus.textContent = '已获取到 code 和版本号，可直接点“更新并保存”。';
        } else if (reason === 'captured') {
            el.codeAutoStatus.textContent = '已获取到 code，但版本号未读取到，请手动补全。';
        } else if (reason === 'timeout') {
            el.codeAutoStatus.textContent = '监听已结束（3分钟超时），可重新点击自动获取。';
        } else {
            el.codeAutoStatus.textContent = '可点击“自动获取 code / 版本号”，最多监听 3 分钟。';
        }
    }

    if (note) {
        setCodeModalError(note);
    }

    if (el.btnCodeAutoStart) {
        el.btnCodeAutoStart.disabled = loading || active || lockedByOther;
        if (active && mine) {
            el.btnCodeAutoStart.textContent = '监听中...';
        } else if (lockedByOther) {
            el.btnCodeAutoStart.textContent = '已上锁';
        } else {
            el.btnCodeAutoStart.textContent = '自动获取 code / 版本号';
        }
    }

    updateCodeModalConfirmState();
}

async function fetchCodeAutoStatus() {
    const result = await apiGet('/api/code-auto/status');
    applyCodeAutoStatus(result);
    renderCodeAutoPanel();
    return result;
}

async function startCodeAutoCapture() {
    if (state.codeAuto.loading) return;
    state.codeAuto.showGuide = true;
    if (el.codeModalInput) {
        el.codeModalInput.blur();
    }
    setCodeModalError('');
    setCodeAutoLoading(true);
    try {
        const result = await apiPost('/api/code-auto/start', {});
        applyCodeAutoStatus(result);
        renderCodeAutoPanel();
        if (result.message) el.actionHint.textContent = result.message;
    } catch (err) {
        setCodeModalError(err.message || '自动获取 code 失败');
        try {
            await fetchCodeAutoStatus();
        } catch (e) {
            // ignore refresh failure
        }
    } finally {
        setCodeAutoLoading(false);
    }
}

function stopCodeAutoCaptureSilently() {
    return apiPost('/api/code-auto/stop', {}).catch(() => null);
}

function stopCodeAutoPolling() {
    const timer = state.codeAuto.pollTimer;
    if (timer) {
        clearInterval(timer);
        state.codeAuto.pollTimer = null;
    }
    state.codeAuto.polling = false;
}

function startCodeAutoPolling() {
    stopCodeAutoPolling();
    state.codeAuto.polling = true;
    const refresh = async () => {
        if (!state.codeModalOpen) return;
        try {
            await fetchCodeAutoStatus();
        } catch (err) {
            if (el.codeAutoStatus) {
                el.codeAutoStatus.textContent = `状态刷新失败: ${err.message}`;
            }
        }
    };
    void refresh();
    state.codeAuto.pollTimer = setInterval(() => {
        void refresh();
    }, 1000);
}

function openCodeModal() {
    if (!el.codeModal || !el.codeModalInput || !el.codeModalVersionInput) return;
    state.codeModalOpen = true;
    state.codeAuto.showGuide = false;
    el.codeModal.hidden = false;
    el.codeModal.setAttribute('aria-hidden', 'false');
    refreshModalBodyLock();
    setCodeModalError('');
    el.codeModalInput.value = '';
    el.codeModalVersionInput.value = state.config ? String(state.config.clientVersion || '') : '';
    renderClientVersionHistoryOptions();
    updateCodeModalConfirmState();
    applyCodeAutoStatus(null);
    renderCodeAutoPanel();
    startCodeAutoPolling();
    window.requestAnimationFrame(() => {
        if (shouldAutoFocusCodeInput()) {
            el.codeModalInput.focus();
            el.codeModalInput.select();
        }
    });
}

function closeCodeModal({ focusTrigger = true } = {}) {
    if (!el.codeModal) return;
    const shouldStopAutoCode = Boolean(state.codeAuto.status && state.codeAuto.status.active && state.codeAuto.status.mine);
    state.codeModalOpen = false;
    state.codeAuto.showGuide = false;
    stopCodeAutoPolling();
    if (shouldStopAutoCode) {
        void stopCodeAutoCaptureSilently();
    }
    applyCodeAutoStatus(null);
    state.codeAuto.loading = false;
    el.codeModal.hidden = true;
    el.codeModal.setAttribute('aria-hidden', 'true');
    refreshModalBodyLock();
    setCodeModalError('');
    if (focusTrigger && el.btnCodePrompt) {
        window.requestAnimationFrame(() => el.btnCodePrompt.focus());
    }
}

function submitCodeModal() {
    if (!el.codeModalInput || !el.codeModalVersionInput) return;
    const value = String(el.codeModalInput.value || '').trim();
    const clientVersion = String(el.codeModalVersionInput.value || '').trim();
    if (!value) {
        setCodeModalError('请输入新的登录 code');
        el.codeModalInput.focus();
        return;
    }
    if (!clientVersion) {
        setCodeModalError('请输入版本号');
        el.codeModalVersionInput.focus();
        return;
    }
    if (el.code) el.code.value = value;
    if (el.clientVersion) el.clientVersion.value = clientVersion;
    closeCodeModal();
    scheduleConfigAutoSave(0, 'code 和版本号已更新并保存为“已就绪”');
}

function setMallBuyModalError(text = '') {
    if (!el.mallBuyModalError) return;
    const msg = String(text || '').trim();
    el.mallBuyModalError.hidden = !msg;
    el.mallBuyModalError.textContent = msg || '请输入有效数量';
}

function refreshMallBuyModalForm() {
    if (!el.mallBuyCountInput) return;

    const running = Boolean(state.runtime && state.runtime.running);
    const diamond = getCurrentDiamondValue();
    const maxByDiamond = Number.isFinite(diamond)
        ? clampInt(Math.floor(diamond / MALL_FERT_10H_UNIT_PRICE), 0, MALL_FERT_BUY_MAX_COUNT, 0)
        : null;

    let count = clampInt(el.mallBuyCountInput.value, 1, MALL_FERT_BUY_MAX_COUNT, 1);
    if (Number.isFinite(maxByDiamond) && maxByDiamond > 0) {
        count = Math.min(count, maxByDiamond);
    }
    if (Number.isFinite(maxByDiamond) && maxByDiamond <= 0) {
        count = 0;
    }
    el.mallBuyCountInput.value = String(count);

    if (el.mallBuyMaxHint) {
        if (Number.isFinite(maxByDiamond)) {
            el.mallBuyMaxHint.textContent = `最多可买：${formatNumber(maxByDiamond)} 个`;
        } else {
            el.mallBuyMaxHint.textContent = '最多可买：点券刷新中';
        }
    }
    if (el.mallBuyCostHint) {
        const spend = Math.max(0, count) * MALL_FERT_10H_UNIT_PRICE;
        el.mallBuyCostHint.textContent = `预计花费：${formatNumber(spend)} 点券`;
    }

    const canSubmit = running && count > 0;
    if (el.btnMallBuyModalConfirm) {
        el.btnMallBuyModalConfirm.disabled = !canSubmit;
        el.btnMallBuyModalConfirm.classList.toggle('is-ready', canSubmit);
    }
}

function openMallBuyModal() {
    if (!el.mallBuyModal || !el.mallBuyCountInput) return;
    state.mallBuyModalOpen = true;
    el.mallBuyModal.hidden = false;
    el.mallBuyModal.setAttribute('aria-hidden', 'false');
    refreshModalBodyLock();
    setMallBuyModalError('');

    const diamond = getCurrentDiamondValue();
    const maxByDiamond = Number.isFinite(diamond)
        ? clampInt(Math.floor(diamond / MALL_FERT_10H_UNIT_PRICE), 0, MALL_FERT_BUY_MAX_COUNT, 0)
        : null;
    const initialCount = Number.isFinite(maxByDiamond) ? Math.max(0, maxByDiamond) : 1;
    el.mallBuyCountInput.value = String(initialCount);
    refreshMallBuyModalForm();

    window.requestAnimationFrame(() => {
        el.mallBuyCountInput.focus();
        el.mallBuyCountInput.select();
    });
}

function closeMallBuyModal({ focusTrigger = true } = {}) {
    if (!el.mallBuyModal) return;
    state.mallBuyModalOpen = false;
    el.mallBuyModal.hidden = true;
    el.mallBuyModal.setAttribute('aria-hidden', 'true');
    refreshModalBodyLock();
    setMallBuyModalError('');
    if (focusTrigger && el.btnOverviewBuy10h) {
        window.requestAnimationFrame(() => el.btnOverviewBuy10h.focus());
    }
}

async function submitMallBuyModal() {
    if (!el.mallBuyCountInput) return;
    const running = Boolean(state.runtime && state.runtime.running);
    if (!running) {
        setMallBuyModalError('bot 未运行，无法购买');
        return;
    }

    const diamond = getCurrentDiamondValue();
    const maxByDiamond = Number.isFinite(diamond)
        ? clampInt(Math.floor(diamond / MALL_FERT_10H_UNIT_PRICE), 0, MALL_FERT_BUY_MAX_COUNT, 0)
        : MALL_FERT_BUY_MAX_COUNT;
    if (maxByDiamond <= 0) {
        setMallBuyModalError('点券不足，无法购买');
        return;
    }

    const count = clampInt(el.mallBuyCountInput.value, 1, maxByDiamond, 1);
    el.mallBuyCountInput.value = String(count);
    setMallBuyModalError('');

    if (el.btnMallBuyModalConfirm) el.btnMallBuyModalConfirm.disabled = true;
    beginMallBuyPending(count);
    if (el.metricDiamond) {
        const optimistic = resolveMetricDiamondDisplay(getCurrentDiamondValue());
        el.metricDiamond.textContent = formatNumber(optimistic);
    }
    try {
        await requestMallBuy10hFertOnce(count);
        closeMallBuyModal();
    } catch (err) {
        resetMallBuyPending();
        setMallBuyModalError(err.message || '购买失败');
    } finally {
        refreshMallBuyModalForm();
    }
}

function setBagItemsModalStatus(text = '') {
    if (!el.bagItemsModalStatus) return;
    el.bagItemsModalStatus.textContent = String(text || '').trim() || '等待加载仓库快照...';
}

function setBagItemsModalError(text = '') {
    if (!el.bagItemsModalError) return;
    const msg = String(text || '').trim();
    el.bagItemsModalError.hidden = !msg;
    el.bagItemsModalError.textContent = msg || '请选择至少一个道具';
}

function bagItemKey(item) {
    return `${Number(item && item.id) || 0}:${Number(item && item.uid) || 0}`;
}

const WAREHOUSE_HIDDEN_PROP_IDS = new Set([
    1, 2, 1001, 1002, 1003, 1004, 1005, 1011, 1012, 1013, 1014, 1015, 1101, 3001, 3002, 4001,
]);
const WAREHOUSE_MUTATION_LABELS = new Map([
    [1, '冰冻'],
    [2, '湿润'],
    [3, '爱心'],
]);

function getWarehouseBaseItemId(itemId, explicitBaseId = 0) {
    const id = Math.floor(Number(itemId) || 0);
    const baseId = Math.floor(Number(explicitBaseId) || 0);
    if (baseId > 0) return baseId;
    if (id >= 1000000) {
        const derived = id % 1000000;
        if (derived > 0) return derived;
    }
    return id;
}

function isWarehouseFruitLike(item = {}) {
    const itemType = Math.floor(Number(item && item.itemType) || 0);
    const baseId = getWarehouseBaseItemId(item && item.id, item && item.baseId);
    return itemType === 6 || (baseId >= 40000 && baseId < 50000);
}

function isWarehouseSeedLike(item = {}) {
    const itemType = Math.floor(Number(item && item.itemType) || 0);
    const baseId = getWarehouseBaseItemId(item && item.id, item && item.baseId);
    const interactionType = String((item && item.interactionType) || '').trim().toLowerCase();
    return itemType === 5 || interactionType === 'plant' || (baseId >= 20000 && baseId < 30000);
}

function getWarehouseMutationLabel(mutantTypes, { isSuperFruit = false } = {}) {
    const rows = Array.isArray(mutantTypes) ? mutantTypes : [];
    if (isSuperFruit) return '黄金';
    if (rows.length <= 0) return '普通';

    const labels = rows
        .map((value) => WAREHOUSE_MUTATION_LABELS.get(Math.floor(Number(value) || 0)) || `变异#${Math.floor(Number(value) || 0)}`)
        .filter((value, index, arr) => value && arr.indexOf(value) === index);

    if (labels.length <= 0) return '普通';
    return labels.join('/');
}

function normalizeWarehouseSnapshotItem(raw, categoryFallback = '') {
    const id = Number(raw && raw.id);
    const count = Number(raw && raw.count);
    const uid = Number(raw && raw.uid);
    if (!Number.isFinite(id) || id <= 0 || !Number.isFinite(count) || count <= 0) return null;

    const baseId = getWarehouseBaseItemId(id, raw && raw.baseId);
    const itemType = Math.floor(Number(raw && raw.itemType) || 0);
    const canUse = Math.floor(Number(raw && raw.canUse) || 0);
    const mutationLabelRaw = String((raw && raw.mutationLabel) || '').trim();
    const mutationKeyRaw = String((raw && raw.mutationKey) || '').trim();
    const mutantTypes = Array.isArray(raw && raw.mutantTypes)
        ? raw.mutantTypes
            .map((value) => Math.floor(Number(value) || 0))
            .filter((value, index, arr) => value > 0 && arr.indexOf(value) === index)
            .sort((a, b) => a - b)
        : [];
    const isSuperFruit = Boolean(raw && raw.isSuperFruit) || (baseId > 0 && baseId !== Math.floor(id) && (itemType === 6 || (baseId >= 40000 && baseId < 50000)));
    const mutationLabel = mutationLabelRaw || getWarehouseMutationLabel(mutantTypes, { isSuperFruit });

    return {
        id: Math.floor(id),
        baseId,
        count: Math.floor(count),
        uid: Number.isFinite(uid) && uid > 0 ? Math.floor(uid) : 0,
        isNew: Boolean(raw && raw.isNew),
        name: String((raw && raw.name) || `道具${Math.floor(id)}`),
        category: String((raw && raw.category) || categoryFallback || '').trim().toLowerCase(),
        itemType,
        interactionType: String((raw && raw.interactionType) || '').trim(),
        canUse,
        mutantTypes,
        mutationLabel,
        mutationKey: mutationKeyRaw || (mutantTypes.length > 0 ? `mutant:${mutantTypes.join('-')}` : 'normal'),
        isSuperFruit,
    };
}

function classifyWarehouseCategory(item = {}) {
    if (item.isSuperFruit && isWarehouseFruitLike(item)) return 'superfruits';
    if (isWarehouseFruitLike(item)) return 'fruits';
    if (isWarehouseSeedLike(item)) return 'seeds';
    return 'props';
}

function normalizeWarehouseCategory(rawCategory, item = {}, fallback = '') {
    const toCategory = (value) => {
        const key = String(value || '').trim().toLowerCase();
        if (!key) return '';
        if (key === 'fruit' || key === 'fruits') return 'fruits';
        if (key === 'superfruit' || key === 'superfruits' || key === 'super_fruits' || key === 'super-fruits') return 'superfruits';
        if (key === 'seed' || key === 'seeds') return 'seeds';
        if (key === 'prop' || key === 'props' || key === 'item' || key === 'items') return 'props';
        return '';
    };
    const explicit = toCategory(rawCategory) || toCategory(fallback);
    const inferred = classifyWarehouseCategory(item);
    if (inferred && inferred !== 'props') {
        if (!explicit || explicit === 'props') return inferred;
        if (explicit === 'fruits' && inferred === 'superfruits') return inferred;
    }
    return explicit || inferred;
}

function shouldHideWarehousePropItem(item) {
    const id = Number(item && item.id);
    const uid = Number(item && item.uid);
    if (!Number.isFinite(id) || id <= 0) return true;
    if (WAREHOUSE_HIDDEN_PROP_IDS.has(Math.floor(id))) return true;
    // 仅展示真实道具堆栈，过滤计数器类字段
    if (!Number.isFinite(uid) || uid <= 0) return true;
    return false;
}

function normalizeWarehouseTab(tab) {
    const key = String(tab || '').trim().toLowerCase();
    if (key === 'superfruits' || key === 'seeds' || key === 'props') return key;
    return 'fruits';
}

function isLikelyUsableWarehouseProp(itemOrId) {
    const id = Number(itemOrId && typeof itemOrId === 'object' ? itemOrId.id : itemOrId);
    if (!Number.isFinite(id) || id <= 0) return false;
    const canUse = Number(itemOrId && typeof itemOrId === 'object' ? itemOrId.canUse : 0);
    const interactionType = String(itemOrId && typeof itemOrId === 'object' ? (itemOrId.interactionType || '') : '').trim().toLowerCase();
    if ([1, 2, 1001, 1002].includes(id)) return false;
    if (Number.isFinite(canUse) && canUse > 0) return true;
    if (interactionType === 'fertilizer' || interactionType === 'fertilizerpro') return true;
    if (id >= 100000 && id < 110000) return true;
    return id >= 80000 && id <= 89999;
}

function getWarehouseMutationSortWeight(label) {
    const text = String(label || '').trim();
    if (!text || text === '普通') return 0;
    if (text === '冰冻') return 1;
    if (text === '湿润') return 2;
    if (text === '爱心') return 3;
    if (text === '黄金') return 4;
    return 9;
}

function compareWarehouseItems(a, b) {
    const nameDiff = String(a && a.name || '').localeCompare(String(b && b.name || ''), 'zh-CN');
    if (nameDiff !== 0) return nameDiff;

    const mutationDiff = getWarehouseMutationSortWeight(a && a.mutationLabel) - getWarehouseMutationSortWeight(b && b.mutationLabel);
    if (mutationDiff !== 0) return mutationDiff;

    const mutationTextDiff = String(a && a.mutationLabel || '').localeCompare(String(b && b.mutationLabel || ''), 'zh-CN');
    if (mutationTextDiff !== 0) return mutationTextDiff;

    const idDiff = (Number(a && a.baseId) || Number(a && a.id) || 0) - (Number(b && b.baseId) || Number(b && b.id) || 0);
    if (idDiff !== 0) return idDiff;

    return (Number(a && a.uid) || 0) - (Number(b && b.uid) || 0);
}

function getWarehouseSnapshotGroups(snapshot = null) {
    const payload = snapshot || state.bagItemsSnapshot || {};
    const groups = {
        fruits: [],
        superFruits: [],
        seeds: [],
        props: [],
    };

    const appendItem = (raw, categoryFallback = '') => {
        const item = normalizeWarehouseSnapshotItem(raw, categoryFallback);
        if (!item) return;
        const category = normalizeWarehouseCategory(item.category, item, categoryFallback);
        item.category = category;
        if (category === 'props' && shouldHideWarehousePropItem(item)) return;
        if (category === 'superfruits') {
            groups.superFruits.push(item);
            return;
        }
        if (category === 'seeds') {
            groups.seeds.push(item);
            return;
        }
        if (category === 'fruits') {
            groups.fruits.push(item);
            return;
        }
        groups.props.push(item);
    };

    if (Array.isArray(payload.fruits)) {
        for (const raw of payload.fruits) appendItem(raw, 'fruits');
    }
    if (Array.isArray(payload.superFruits)) {
        for (const raw of payload.superFruits) appendItem(raw, 'superfruits');
    }
    if (Array.isArray(payload.seeds)) {
        for (const raw of payload.seeds) appendItem(raw, 'seeds');
    }
    if (Array.isArray(payload.props)) {
        for (const raw of payload.props) appendItem(raw, 'props');
    }

    // 向后兼容旧快照格式
    if ((groups.fruits.length + groups.superFruits.length + groups.seeds.length + groups.props.length) === 0 && Array.isArray(payload.items)) {
        for (const raw of payload.items) {
            appendItem(raw, '');
        }
    }

    const mergeForDisplay = (items, { withMutation = false } = {}) => {
        const list = Array.isArray(items) ? items : [];
        const merged = new Map();
        for (const item of list) {
            const id = Number(item && (item.baseId || item.id));
            const count = Number(item && item.count);
            if (!Number.isFinite(id) || id <= 0 || !Number.isFinite(count) || count <= 0) continue;
            const mutationKey = withMutation ? String((item && item.mutationKey) || 'normal') : 'plain';
            const key = `${Math.floor(id)}:${mutationKey}`;
            const existed = merged.get(key);
            if (existed) {
                existed.count += Math.floor(count);
                existed.isNew = existed.isNew || Boolean(item && item.isNew);
                continue;
            }
            merged.set(key, {
                ...item,
                uid: 0,
                baseId: Math.floor(id),
                count: Math.floor(count),
            });
        }
        return Array.from(merged.values()).sort(compareWarehouseItems);
    };

    // 果实按“基础物品 + 变异类型”合并，种子按基础物品合并；道具保留 uid 维度用于勾选使用
    groups.fruits = mergeForDisplay(groups.fruits, { withMutation: true });
    groups.superFruits = mergeForDisplay(groups.superFruits, { withMutation: true });
    groups.seeds = mergeForDisplay(groups.seeds);
    groups.props = groups.props.sort(compareWarehouseItems);

    return groups;
}

function getWarehouseTabItems(tab = state.warehouseActiveTab) {
    const groups = getWarehouseSnapshotGroups();
    const key = normalizeWarehouseTab(tab);
    if (key === 'superfruits') return groups.superFruits;
    if (key === 'seeds') return groups.seeds;
    if (key === 'props') return groups.props;
    return groups.fruits;
}

function renderWarehouseTabButtons() {
    const active = normalizeWarehouseTab(state.warehouseActiveTab);
    const list = [
        { key: 'fruits', el: el.btnWarehouseTabFruits },
        { key: 'superfruits', el: el.btnWarehouseTabSuperFruits },
        { key: 'seeds', el: el.btnWarehouseTabSeeds },
        { key: 'props', el: el.btnWarehouseTabProps },
    ];
    for (const one of list) {
        if (!one.el) continue;
        const on = one.key === active;
        one.el.classList.toggle('is-active', on);
        one.el.setAttribute('aria-selected', on ? 'true' : 'false');
    }
}

function setWarehouseTab(tab) {
    state.warehouseActiveTab = normalizeWarehouseTab(tab);
    setBagItemsModalError('');
    renderWarehouseTabButtons();
    renderBagItemsModalList();
}

function updateBagItemsModalConfirmState() {
    const currentTab = normalizeWarehouseTab(state.warehouseActiveTab);
    const isPropsTab = currentTab === 'props';
    const propsItems = getWarehouseTabItems('props');
    const running = Boolean(state.runtime && state.runtime.running);
    const hasSelection = state.bagItemsSelection && state.bagItemsSelection.size > 0 && propsItems.length > 0;
    if (el.btnBagItemsRefresh) el.btnBagItemsRefresh.disabled = state.bagItemsLoading;
    if (el.btnBagItemsSelectAll) {
        el.btnBagItemsSelectAll.hidden = !isPropsTab;
        el.btnBagItemsSelectAll.disabled = state.bagItemsLoading || !isPropsTab || propsItems.length <= 0;
    }
    if (el.btnBagItemsClear) {
        el.btnBagItemsClear.hidden = !isPropsTab;
        el.btnBagItemsClear.disabled = state.bagItemsLoading || !isPropsTab || propsItems.length <= 0;
    }
    if (el.btnBagItemsModalConfirm) {
        el.btnBagItemsModalConfirm.hidden = !isPropsTab;
        el.btnBagItemsModalConfirm.disabled = !isPropsTab || !running || !hasSelection || state.bagItemsLoading;
        el.btnBagItemsModalConfirm.classList.toggle('is-ready', isPropsTab && running && hasSelection && !state.bagItemsLoading);
    }
}

function renderBagItemsModalList() {
    if (!el.bagItemsModalList) return;
    const tab = normalizeWarehouseTab(state.warehouseActiveTab);
    const items = getWarehouseTabItems(tab);

    el.bagItemsModalList.innerHTML = '';
    if (items.length === 0) {
        const empty = document.createElement('p');
        empty.className = 'bag-item-empty';
        if (tab === 'superfruits') {
            empty.textContent = '超变果实页为空。';
        } else if (tab === 'seeds') {
            empty.textContent = '种子页为空。';
        } else if (tab === 'props') {
            empty.textContent = '道具页为空。';
        } else {
            empty.textContent = '果实页为空。';
        }
        el.bagItemsModalList.appendChild(empty);
        updateBagItemsModalConfirmState();
        return;
    }

    const frag = document.createDocumentFragment();
    for (const item of items) {
        const row = document.createElement(tab === 'props' ? 'label' : 'div');
        row.className = 'bag-item-row';

        const main = document.createElement('div');
        main.className = 'bag-item-main';
        const name = document.createElement('p');
        name.className = 'bag-item-name';
        name.textContent = item.name;
        const meta = document.createElement('p');
        meta.className = 'bag-item-meta';
        if (tab === 'props') {
            const autoHint = isLikelyUsableWarehouseProp(item) ? '可勾选尝试打开' : '可能不可直接打开';
            meta.textContent = `ID=${item.id} UID=${item.uid || 0} · ${autoHint}${item.isNew ? ' · 新道具' : ''}`;
        } else if (tab === 'seeds') {
            meta.textContent = `ID=${item.baseId || item.id}`;
        } else {
            const idText = item.id !== item.baseId
                ? `基础ID=${item.baseId} · 原始ID=${item.id}`
                : `ID=${item.baseId || item.id}`;
            meta.textContent = `变异类型：${item.mutationLabel || '普通'} · ${idText}`;
        }
        main.appendChild(name);
        main.appendChild(meta);

        const count = document.createElement('strong');
        count.className = 'bag-item-count';
        count.textContent = `x${formatNumber(item.count)}`;

        if (tab === 'props') {
            const key = bagItemKey(item);
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.dataset.itemKey = key;
            checkbox.checked = state.bagItemsSelection.has(key);
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    state.bagItemsSelection.add(key);
                } else {
                    state.bagItemsSelection.delete(key);
                }
                updateBagItemsModalConfirmState();
                setBagItemsModalError('');
            });
            row.appendChild(checkbox);
        } else {
            const spacer = document.createElement('span');
            spacer.style.width = '15px';
            spacer.style.height = '15px';
            row.appendChild(spacer);
        }
        row.appendChild(main);
        row.appendChild(count);
        frag.appendChild(row);
    }

    el.bagItemsModalList.appendChild(frag);
    updateBagItemsModalConfirmState();
}

function applyBagItemsSnapshot(snapshot, { preserveSelection = false } = {}) {
    state.bagItemsSnapshot = snapshot && typeof snapshot === 'object' ? snapshot : null;
    const props = getWarehouseSnapshotGroups(state.bagItemsSnapshot).props;

    if (preserveSelection) {
        const next = new Set();
        for (const item of props) {
            const key = bagItemKey(item);
            if (state.bagItemsSelection.has(key)) next.add(key);
        }
        state.bagItemsSelection = next;
    } else {
        state.bagItemsSelection = new Set(
            props
                .filter((item) => isLikelyUsableWarehouseProp(item))
                .map((item) => bagItemKey(item))
        );
    }

    const updatedAtText = snapshot && snapshot.updatedAt ? String(snapshot.updatedAt) : '';
    const groups = getWarehouseSnapshotGroups(state.bagItemsSnapshot);
    const counts = `果实${groups.fruits.length}种 / 超变${groups.superFruits.length}种 / 种子${groups.seeds.length}种 / 道具${groups.props.length}种`;
    setBagItemsModalStatus(updatedAtText ? `仓库快照时间：${updatedAtText} · ${counts}` : `仓库快照已加载 · ${counts}`);
    renderBagItemsModalList();
}

async function waitForFreshBagSnapshot(afterMs, timeoutMs = 10000) {
    const deadline = Date.now() + Math.max(1500, timeoutMs);
    const minMs = Math.max(0, Number(afterMs) || 0);
    let lastSnapshot = null;
    while (Date.now() < deadline) {
        const snapshot = await fetchBagSnapshot();
        lastSnapshot = snapshot;
        const updatedAtMs = Number(snapshot && snapshot.updatedAtMs);
        if (snapshot && snapshot.hasSnapshot && Number.isFinite(updatedAtMs) && updatedAtMs >= minMs) {
            return snapshot;
        }
        await delayMs(320);
    }
    return lastSnapshot;
}

async function refreshBagItemsModalSnapshot({ forceFresh = true, preserveSelection = false } = {}) {
    if (state.bagItemsLoading) return;
    state.bagItemsLoading = true;
    updateBagItemsModalConfirmState();
    setBagItemsModalError('');
    setBagItemsModalStatus(forceFresh ? '正在刷新仓库快照...' : '正在读取仓库快照...');
    try {
        let snapshot;
        if (forceFresh) {
            const req = await requestBagSnapshotOnce();
            const requestedAtMs = Number(req && req.requestedAtMs) || Date.now();
            snapshot = await waitForFreshBagSnapshot(requestedAtMs);
        } else {
            snapshot = await fetchBagSnapshot();
        }

        if (!snapshot || !snapshot.hasSnapshot) {
            throw new Error((snapshot && snapshot.message) || '暂未获取到仓库快照');
        }
        applyBagItemsSnapshot(snapshot, { preserveSelection });
        setBagItemsModalError('');
    } catch (err) {
        setBagItemsModalStatus('仓库快照加载失败');
        setBagItemsModalError(err.message || '读取仓库快照失败');
    } finally {
        state.bagItemsLoading = false;
        updateBagItemsModalConfirmState();
    }
}

function openBagItemsModal() {
    if (!el.bagItemsModal) return;
    state.bagItemsModalOpen = true;
    el.bagItemsModal.hidden = false;
    el.bagItemsModal.setAttribute('aria-hidden', 'false');
    refreshModalBodyLock();
    state.warehouseActiveTab = 'fruits';
    setBagItemsModalError('');
    setBagItemsModalStatus('正在加载仓库...');
    if (el.bagItemsModalList) el.bagItemsModalList.innerHTML = '';
    state.bagItemsSelection = new Set();
    renderWarehouseTabButtons();
    renderBagItemsModalList();
    void refreshBagItemsModalSnapshot({ forceFresh: true, preserveSelection: false });
}

function closeBagItemsModal({ focusTrigger = true } = {}) {
    if (!el.bagItemsModal) return;
    state.bagItemsModalOpen = false;
    el.bagItemsModal.hidden = true;
    el.bagItemsModal.setAttribute('aria-hidden', 'true');
    refreshModalBodyLock();
    setBagItemsModalError('');
    if (focusTrigger && el.btnBagUseAll) {
        window.requestAnimationFrame(() => el.btnBagUseAll.focus());
    }
}

function collectSelectedBagItemsForSubmit() {
    const items = getWarehouseSnapshotGroups(state.bagItemsSnapshot).props;
    const selected = [];
    for (const item of items) {
        const key = bagItemKey(item);
        if (!state.bagItemsSelection.has(key)) continue;
        selected.push({
            id: item.id,
            uid: item.uid || 0,
            count: item.count,
            name: item.name,
        });
    }
    return selected;
}

async function submitBagItemsModal() {
    const selected = collectSelectedBagItemsForSubmit();
    if (selected.length <= 0) {
        setBagItemsModalError('请先勾选至少一个道具');
        return;
    }

    setBagItemsModalError('');
    if (el.btnBagItemsModalConfirm) el.btnBagItemsModalConfirm.disabled = true;
    try {
        await requestUseSelectedBagItemsOnce(selected);
        closeBagItemsModal();
    } catch (err) {
        setBagItemsModalError(err.message || '请求失败');
    } finally {
        updateBagItemsModalConfirmState();
    }
}

function setFriendAllDayState(enabled, { syncInputs = true } = {}) {
    if (!el.friendActiveAllDay) return;
    const on = Boolean(enabled);
    el.friendActiveAllDay.checked = on;
    if (syncInputs) {
        if (on) {
            if (el.friendActiveStart) el.friendActiveStart.value = '00:00';
            if (el.friendActiveEnd) el.friendActiveEnd.value = '00:00';
        }
    }
    if (el.friendActiveStart) el.friendActiveStart.disabled = on;
    if (el.friendActiveEnd) el.friendActiveEnd.disabled = on;
}

function setFriendApplyAllDayState(enabled, { syncInputs = true } = {}) {
    if (!el.friendApplyAllDay) return;
    const on = Boolean(enabled);
    el.friendApplyAllDay.checked = on;
    if (syncInputs) {
        if (on) {
            if (el.friendApplyActiveStart) el.friendApplyActiveStart.value = '00:00';
            if (el.friendApplyActiveEnd) el.friendApplyActiveEnd.value = '00:00';
        }
    }
    if (el.friendApplyActiveStart) el.friendApplyActiveStart.disabled = on;
    if (el.friendApplyActiveEnd) el.friendApplyActiveEnd.disabled = on;
}

function collectConfigPayload() {
    const intervalSec = setIntervalControl('farm', el.intervalSec ? el.intervalSec.value : 1);
    const friendIntervalSec = setIntervalControl('friend', el.friendIntervalSec ? el.friendIntervalSec.value : 1);
    const landRefreshIntervalSec = setIntervalControl('landRefresh', el.landRefreshIntervalSec ? el.landRefreshIntervalSec.value : 5);
    const friendActiveAllDay = Boolean(el.friendActiveAllDay && el.friendActiveAllDay.checked);
    const friendActiveStart = el.friendActiveStart ? el.friendActiveStart.value.trim() : '';
    const friendActiveEnd = el.friendActiveEnd ? el.friendActiveEnd.value.trim() : '';
    const friendApplyAllDay = Boolean(el.friendApplyAllDay && el.friendApplyAllDay.checked);
    const friendApplyActiveStart = el.friendApplyActiveStart ? el.friendApplyActiveStart.value.trim() : '';
    const friendApplyActiveEnd = el.friendApplyActiveEnd ? el.friendApplyActiveEnd.value.trim() : '';

    return {
        platform: 'wx',
        code: el.code ? el.code.value.trim() : '',
        clientVersion: el.clientVersion ? el.clientVersion.value.trim() : '',
        performanceMode: normalizePerformanceMode(el.performanceMode ? el.performanceMode.value : state.performanceMode),
        intervalSec,
        friendIntervalSec,
        fastHarvest: Boolean(el.fastHarvest ? el.fastHarvest.checked : true),
        autoFertilize: Boolean(el.autoFertilize ? el.autoFertilize.checked : true),
        friendActionSteal: Boolean(el.friendActionSteal && el.friendActionSteal.checked),
        friendActionCare: Boolean(el.friendActionCare && el.friendActionCare.checked),
        friendActionPrank: Boolean(el.friendActionPrank && el.friendActionPrank.checked),
        stealLevelThreshold: getStealLevelThresholdValue(),
        friendAutoDeleteNoStealEnabled: Boolean(el.friendAutoDeleteNoStealEnabled && el.friendAutoDeleteNoStealEnabled.checked),
        friendAutoDeleteNoStealDays: clampInt(el.friendAutoDeleteNoStealDays ? el.friendAutoDeleteNoStealDays.value : 7, 1, 3650, 7),
        friendActiveAllDay,
        friendActiveStart,
        friendActiveEnd,
        friendApplyAllDay,
        friendApplyActiveStart,
        friendApplyActiveEnd,
        landRefreshIntervalSec,
        preferredSeedId: Number(el.preferredSeedId ? el.preferredSeedId.value : 0) || 0,
        allowMulti: el.allowMulti.checked,
        extraArgs: el.extraArgs.value.trim(),
        farmCalcUseAutoLands: Boolean(el.farmCalcUseAutoLands && el.farmCalcUseAutoLands.checked),
        farmCalcManualLands: getFarmCalcManualLandsValue(),
    };
}

function normalizeMainTab(tabKey) {
    const key = String(tabKey || '').trim().toLowerCase();
    if (key === 'logs' || key === 'settings' || key === 'friends' || key === 'stats') return key;
    return 'overview';
}

function activateMainTab(tabKey, { syncHash = true } = {}) {
    if (!el.tabButtons.length || !el.tabPanels.length) return;
    const target = normalizeMainTab(tabKey);
    state.activeTab = target;

    for (const btn of el.tabButtons) {
        const selected = btn.dataset.tabTarget === target;
        btn.classList.toggle('is-active', selected);
        btn.setAttribute('aria-selected', selected ? 'true' : 'false');
        btn.setAttribute('tabindex', selected ? '0' : '-1');
    }

    for (const panel of el.tabPanels) {
        const visible = panel.dataset.tabPanel === target;
        panel.classList.toggle('is-active', visible);
        panel.hidden = !visible;
    }

    if (syncHash) {
        const nextHash = `#${target}`;
        if (window.location.hash !== nextHash) {
            window.history.replaceState(null, '', nextHash);
        }
    }

    if (target === 'logs') {
        renderLogs();
    } else if (target === 'stats') {
        void refreshOverviewStats({ quiet: true });
    } else if (target === 'friends') {
        void refreshFriendInsights({ quiet: true });
    }
}

function bindMainTabs() {
    if (!el.mainTabs || !el.tabButtons.length) return;

    for (const btn of el.tabButtons) {
        btn.setAttribute('role', 'tab');
    }

    el.mainTabs.addEventListener('click', (ev) => {
        const btn = ev.target.closest('[data-tab-target]');
        if (!btn) return;
        activateMainTab(btn.dataset.tabTarget, { syncHash: true });
    });

    el.mainTabs.addEventListener('keydown', (ev) => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(ev.key)) return;
        const currentIndex = el.tabButtons.findIndex((btn) => btn.classList.contains('is-active'));
        if (currentIndex < 0) return;
        ev.preventDefault();

        let nextIndex = currentIndex;
        if (ev.key === 'ArrowLeft') nextIndex = (currentIndex - 1 + el.tabButtons.length) % el.tabButtons.length;
        if (ev.key === 'ArrowRight') nextIndex = (currentIndex + 1) % el.tabButtons.length;
        if (ev.key === 'Home') nextIndex = 0;
        if (ev.key === 'End') nextIndex = el.tabButtons.length - 1;

        const next = el.tabButtons[nextIndex];
        if (!next) return;
        activateMainTab(next.dataset.tabTarget, { syncHash: true });
        next.focus();
    });

    activateMainTab(normalizeMainTab(window.location.hash.replace('#', '')), { syncHash: false });
    window.addEventListener('hashchange', () => {
        activateMainTab(normalizeMainTab(window.location.hash.replace('#', '')), { syncHash: false });
    });
}

function scheduleConfigAutoSave(delayMs = 260, reason = '配置已自动保存') {
    state.configSavePendingReason = reason;
    if (state.configAutoSaveTimer) {
        clearTimeout(state.configAutoSaveTimer);
        state.configAutoSaveTimer = null;
    }
    state.configAutoSaveTimer = setTimeout(() => {
        state.configAutoSaveTimer = null;
        void flushConfigAutoSave();
    }, Math.max(0, delayMs));
}

async function flushConfigAutoSave() {
    if (state.configSaveInFlight) {
        state.configSaveQueued = true;
        return;
    }
    state.configSaveInFlight = true;
    try {
        el.actionHint.textContent = '正在自动保存配置...';
        await saveConfig({ auto: true, hint: state.configSavePendingReason || '配置已自动保存' });
    } catch (err) {
        el.actionHint.textContent = `自动保存失败: ${err.message}`;
    } finally {
        state.configSaveInFlight = false;
        if (state.configSaveQueued) {
            state.configSaveQueued = false;
            scheduleConfigAutoSave(80, state.configSavePendingReason || '配置已自动保存');
        }
    }
}

function promptAndUpdateCode() {
    openCodeModal();
}

function getIntervalBounds(type) {
    if (type === 'friend') {
        return { min: 0, max: 300 };
    }
    return { min: 0, max: 300 };
}

function setIntervalControl(type, value) {
    const fallbackByType = {
        farm: 1,
        friend: 1,
        landRefresh: 5,
    };
    const { min, max } = getIntervalBounds(type);
    const normalized = clampInt(value, min, max, fallbackByType[type] || min);
    if (type === 'farm') {
        if (el.intervalSec) el.intervalSec.value = normalized;
        if (el.intervalSecRange) el.intervalSecRange.value = normalized;
        if (el.intervalSecDisplay) el.intervalSecDisplay.textContent = `${normalized}s`;
        return normalized;
    }
    if (type === 'friend') {
        if (el.friendIntervalSec) el.friendIntervalSec.value = normalized;
        if (el.friendIntervalSecRange) el.friendIntervalSecRange.value = normalized;
        if (el.friendIntervalSecDisplay) el.friendIntervalSecDisplay.textContent = `${normalized}s`;
        return normalized;
    }
    if (el.landRefreshIntervalSec) el.landRefreshIntervalSec.value = normalized;
    if (el.landRefreshIntervalSecRange) el.landRefreshIntervalSecRange.value = normalized;
    if (el.landRefreshIntervalSecDisplay) el.landRefreshIntervalSecDisplay.textContent = `${normalized}s`;
    return normalized;
}

function bindIntervalControl(type) {
    const map = {
        farm: { numberEl: el.intervalSec, rangeEl: el.intervalSecRange, fallback: 1 },
        friend: { numberEl: el.friendIntervalSec, rangeEl: el.friendIntervalSecRange, fallback: 1 },
        landRefresh: { numberEl: el.landRefreshIntervalSec, rangeEl: el.landRefreshIntervalSecRange, fallback: 5 },
    };
    const conf = map[type];
    const numberEl = conf ? conf.numberEl : null;
    const rangeEl = conf ? conf.rangeEl : null;
    const fallback = conf ? conf.fallback : 1;
    const { min, max } = getIntervalBounds(type);
    if (!numberEl || !rangeEl) return;

    const syncFrom = (source) => {
        const currentRaw = Number(numberEl.value);
        const current = Number.isFinite(currentRaw) ? currentRaw : fallback;
        const next = clampInt(source.value, min, max, current);
        setIntervalControl(type, next);
    };

    rangeEl.addEventListener('input', () => {
        syncFrom(rangeEl);
        scheduleConfigAutoSave(220);
    });
    rangeEl.addEventListener('change', () => {
        syncFrom(rangeEl);
        scheduleConfigAutoSave(80);
    });
    numberEl.addEventListener('input', () => {
        syncFrom(numberEl);
        scheduleConfigAutoSave(220);
    });
    numberEl.addEventListener('change', () => {
        syncFrom(numberEl);
        scheduleConfigAutoSave(80);
    });
}

function formatExpPerHour(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return '-';
    return `${n.toFixed(2)} exp/h`;
}

function formatSeasonGrowText(item) {
    const seasons = Number(item && item.seasons);
    const seasonText = Number.isInteger(seasons) && seasons > 0 ? `${seasons}季` : '';
    const growTimeText = String(item && item.growTimeStr || '').trim();
    return [seasonText, growTimeText].filter(Boolean).join(' ');
}

function formatFarmCalcSeedLabel(item) {
    if (!item) return '-';
    const lv = Number(item.requiredLevel);
    const lvText = Number.isInteger(lv) && lv > 0 ? `Lv${lv}` : 'Lv?';
    const detail = formatSeasonGrowText(item);
    return `${item.name || '-'} (${lvText})${detail ? ` ${detail}` : ''}`;
}

function getFarmCalcManualLandsValue() {
    if (!el.farmCalcManualLands) return 24;
    const value = clampInt(el.farmCalcManualLands.value, 1, 500, 24);
    el.farmCalcManualLands.value = value;
    return value;
}

function ensurePreferredSeedOption(seedId, name) {
    if (!el.preferredSeedId) return;
    const val = String(Number(seedId || 0));
    if (!val || val === '0') return;
    const hasOption = Array.from(el.preferredSeedId.options).some((opt) => opt.value === val);
    if (hasOption) return;
    const fallback = document.createElement('option');
    fallback.value = val;
    fallback.textContent = name ? `${name}(${val})` : `自定义种子(${val})`;
    el.preferredSeedId.appendChild(fallback);
}

function setFarmCalcStatus(text, isError = false) {
    if (!el.farmCalcStatus) return;
    el.farmCalcStatus.textContent = text;
    el.farmCalcStatus.style.color = isError ? 'var(--danger)' : 'var(--brand-strong)';
}

function renderFarmCalcResult(payload) {
    const recommendation = payload && payload.recommendation ? payload.recommendation : null;
    const bestNo = recommendation ? recommendation.bestNoFert : null;
    const bestFert = recommendation ? recommendation.bestNormalFert : null;
    const topFert = recommendation && Array.isArray(recommendation.candidatesNormalFert)
        ? recommendation.candidatesNormalFert.slice(0, 5)
        : [];

    if (el.farmCalcLevelHint) {
        const levelVal = payload && payload.level ? Number(payload.level.value || 0) : 0;
        const source = payload && payload.level ? payload.level.source : '';
        const sourceText = source === 'metrics' ? '自动' : '手动';
        el.farmCalcLevelHint.textContent = levelVal > 0 ? `等级 Lv${levelVal} (${sourceText})` : '等级 --';
    }

    if (el.farmCalcInputHint) {
        el.farmCalcInputHint.textContent = '按固定 24 地块计算推荐。';
    }

    if (el.farmCalcNoFertName) {
        el.farmCalcNoFertName.textContent = bestNo ? formatFarmCalcSeedLabel(bestNo) : '-';
    }
    if (el.farmCalcNoFertExp) {
        el.farmCalcNoFertExp.textContent = formatExpPerHour(bestNo && bestNo.expPerHour);
    }
    if (el.farmCalcFertName) {
        el.farmCalcFertName.textContent = bestFert ? formatFarmCalcSeedLabel(bestFert) : '-';
    }
    if (el.farmCalcFertExp) {
        el.farmCalcFertExp.textContent = formatExpPerHour(bestFert && bestFert.expPerHour);
    }

    if (el.farmCalcTopList) {
        el.farmCalcTopList.innerHTML = '';
        if (topFert.length === 0) {
            const empty = document.createElement('p');
            empty.className = 'farmcalc-note';
            empty.textContent = '暂无推荐结果';
            el.farmCalcTopList.appendChild(empty);
        } else {
            const frag = document.createDocumentFragment();
            topFert.forEach((item, index) => {
                const row = document.createElement('div');
                row.className = 'farmcalc-top-item';

                const rank = document.createElement('span');
                rank.className = 'farmcalc-top-rank';
                rank.textContent = String(index + 1);

                const name = document.createElement('span');
                name.className = 'farmcalc-top-name';
                name.textContent = formatFarmCalcSeedLabel(item);

                const exp = document.createElement('span');
                exp.className = 'farmcalc-top-exp';
                exp.textContent = formatExpPerHour(item.expPerHour);

                row.appendChild(rank);
                row.appendChild(name);
                row.appendChild(exp);
                frag.appendChild(row);
            });
            el.farmCalcTopList.appendChild(frag);
        }
    }
}

function buildFarmCalcRequestPath() {
    const params = new URLSearchParams();
    params.set('top', '8');
    params.set('useAutoLands', '0');
    params.set('manualLands', '24');
    return `/api/farmcalc/recommendation?${params.toString()}`;
}

function scheduleFarmCalcRefresh(delayMs = 350) {
    if (state.farmCalcRefreshTimer) {
        clearTimeout(state.farmCalcRefreshTimer);
    }
    state.farmCalcRefreshTimer = setTimeout(() => {
        state.farmCalcRefreshTimer = null;
        refreshFarmCalcRecommendation({ quiet: true });
    }, delayMs);
}

async function refreshFarmCalcRecommendation(options = {}) {
    if (!el.farmCalcPanel) return;
    const quiet = Boolean(options.quiet);
    const requestId = state.farmCalcRequestId + 1;
    state.farmCalcRequestId = requestId;

    if (!quiet) {
        setFarmCalcStatus('正在计算推荐...');
    }

    try {
        const payload = await apiGet(buildFarmCalcRequestPath());
        if (state.farmCalcRequestId !== requestId) return;

        state.farmCalcResult = payload;
        renderFarmCalcResult(payload);
        setFarmCalcStatus('推荐已更新');
    } catch (err) {
        if (state.farmCalcRequestId !== requestId) return;
        state.farmCalcResult = null;
        renderFarmCalcResult(null);
        setFarmCalcStatus(`推荐失败: ${err.message}`, true);
    }
}

async function applyBestFarmCalcSeed() {
    const payload = state.farmCalcResult;
    const recommendation = payload && payload.recommendation ? payload.recommendation : null;
    const bestFert = recommendation ? recommendation.bestNormalFert : null;
    const bestNo = recommendation ? recommendation.bestNoFert : null;
    const target = bestFert || bestNo;
    if (!target || !target.seedId) {
        throw new Error('当前没有可应用的推荐作物');
    }

    ensurePreferredSeedOption(target.seedId, target.name);
    if (el.preferredSeedId) {
        el.preferredSeedId.value = String(target.seedId);
    }
    await saveConfig();
    el.actionHint.textContent = `已应用推荐作物: ${target.name} (${target.seedId})`;
}

function getLogTag(entry) {
    if (!entry || !entry.tag) return '系统';
    return entry.tag;
}

function getDynamicLogTags() {
    const tagSet = new Set();
    for (const entry of state.logs) {
        const tag = String(getLogTag(entry) || '').trim();
        if (!tag || tag === 'ALL' || tag === 'ERROR') continue;
        tagSet.add(tag);
    }
    return Array.from(tagSet).sort((a, b) => a.localeCompare(b, 'zh-Hans-CN', { sensitivity: 'base' }));
}

function renderLogFilterChips() {
    if (!el.filterRow) return;
    const tags = ['ALL', ...getDynamicLogTags(), 'ERROR'];
    const signature = tags.join('|');
    if (signature === state.logFilterSignature) {
        const hasActive = tags.includes(state.activeTag);
        if (!hasActive) state.activeTag = 'ALL';
        for (const chip of el.filterRow.querySelectorAll('.filter-chip')) {
            chip.classList.toggle('active', chip.dataset.tag === state.activeTag);
        }
        return;
    }

    state.logFilterSignature = signature;
    if (!tags.includes(state.activeTag)) state.activeTag = 'ALL';

    const fragment = document.createDocumentFragment();
    for (const tag of tags) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = `filter-chip${state.activeTag === tag ? ' active' : ''}`;
        btn.dataset.tag = tag;
        btn.textContent = tag === 'ALL' ? '全部' : (tag === 'ERROR' ? '错误' : tag);
        fragment.appendChild(btn);
    }
    el.filterRow.innerHTML = '';
    el.filterRow.appendChild(fragment);
}

function matchLogFilter(entry) {
    const text = (entry.text || '').toLowerCase();
    const tag = getLogTag(entry);

    if (state.activeTag === 'ERROR') {
        const isErrorLevel = entry.level === 'error' || entry.level === 'warn';
        const hasErrWord = /失败|错误|超时|⚠/i.test(entry.text || '');
        if (!isErrorLevel && !hasErrWord) return false;
    } else if (state.activeTag !== 'ALL' && tag !== state.activeTag) {
        return false;
    }

    if (state.search && !text.includes(state.search.toLowerCase())) {
        return false;
    }

    return true;
}

function renderLogs() {
    renderLogFilterChips();
    const logs = state.logs.filter(matchLogFilter);
    el.logList.innerHTML = '';

    if (logs.length === 0) {
        const empty = document.createElement('p');
        empty.className = 'empty-log';
        empty.textContent = '暂无匹配日志';
        el.logList.appendChild(empty);
        return;
    }

    const fragment = document.createDocumentFragment();
    for (const entry of logs.slice(-600)) {
        const line = document.createElement('div');
        line.className = `log-line ${entry.level || 'info'}`;
        line.textContent = entry.text;
        fragment.appendChild(line);
    }
    el.logList.appendChild(fragment);

    if (el.autoScroll.checked) {
        el.logList.scrollTop = el.logList.scrollHeight;
    }
}

function updateOverviewBuy10hVisibility(options = {}) {
    if (!el.btnOverviewBuy10h) return;

    const isOnline = options.isOnline !== undefined
        ? Boolean(options.isOnline)
        : Boolean(state.runtime && state.runtime.running);
    const autoFertilizeEnabled = el.autoFertilize
        ? Boolean(el.autoFertilize.checked)
        : !state.config || state.config.autoFertilize !== false;

    el.btnOverviewBuy10h.hidden = autoFertilizeEnabled;
    el.btnOverviewBuy10h.disabled = autoFertilizeEnabled || !isOnline;
    el.btnOverviewBuy10h.title = autoFertilizeEnabled
        ? '自动施肥开启时隐藏手动购买入口'
        : (isOnline ? '购买10小时普通化肥' : 'bot 未运行，无法执行购买请求');

    if (autoFertilizeEnabled && state.mallBuyModalOpen) {
        closeMallBuyModal({ focusTrigger: false });
    }
}

function renderFertilizerMetric(metrics) {
    if (!el.metricFertilizer) return;

    const rawCount = metrics && metrics.normalFertilizerCount;
    const count = Number(rawCount);
    const hasCount = Number.isFinite(count) && count >= 0;
    const state = metrics && metrics.normalFertilizerState ? String(metrics.normalFertilizerState) : '';

    el.metricFertilizer.classList.remove('metric-fertilizer-ok', 'metric-fertilizer-low');
    el.metricFertilizer.title = '';

    if (hasCount) {
        if (count > 0) {
            el.metricFertilizer.textContent = `剩余 ${formatFertilizerHoursByCount(count)}`;
            el.metricFertilizer.title = `普通化肥可加速时长（精确计时）：${formatFertilizerRemainByCount(count)}`;
            el.metricFertilizer.classList.add('metric-fertilizer-ok');
        } else {
            el.metricFertilizer.textContent = '肥料不足';
            el.metricFertilizer.classList.add('metric-fertilizer-low');
        }
        return;
    }

    if (state === 'ok') {
        el.metricFertilizer.textContent = '肥料充足';
        el.metricFertilizer.classList.add('metric-fertilizer-ok');
        return;
    }
    if (state === 'low') {
        el.metricFertilizer.textContent = '肥料不足';
        el.metricFertilizer.classList.add('metric-fertilizer-low');
        return;
    }

    el.metricFertilizer.textContent = '-';
}

function renderRuntime(runtime) {
    if (!runtime) return;
    state.runtime = runtime;

    const isOnline = Boolean(runtime.running);
    if (!isOnline && state.mallBuyPending) {
        resetMallBuyPending();
    }
    el.statusBadge.textContent = isOnline ? '在线' : '离线';
    el.statusBadge.classList.toggle('online', isOnline);
    el.statusBadge.classList.toggle('offline', !isOnline);

    updateToggleButton(isOnline);
    el.runtimeText.textContent = isOnline ? '运行中' : '未运行';
    el.metricPid.textContent = runtime.pid || '-';
    el.metricUptime.textContent = formatDuration(runtime.uptimeSec || 0);

    const metrics = runtime.metrics || {};
    el.metricLevel.textContent = formatNumber(metrics.level);
    el.metricGold.textContent = formatNumber(metrics.gold);
    if (el.metricDiamond) {
        const diamondDisplay = resolveMetricDiamondDisplay(metrics.diamond);
        el.metricDiamond.textContent = formatNumber(diamondDisplay);
    }
    renderFertilizerMetric(metrics);

    const currentFarmCalcSig = `${Number(metrics.level || 0)}:${Number(metrics.landCount || 0)}`;
    if (currentFarmCalcSig !== state.farmCalcInputSig) {
        state.farmCalcInputSig = currentFarmCalcSig;
        scheduleFarmCalcRefresh(500);
    }

    if (metrics.expCurrent !== null && metrics.expNeeded !== null) {
        el.metricExp.textContent = `${formatNumber(metrics.expCurrent)}/${formatNumber(metrics.expNeeded)}`;
        const current = Number(metrics.expCurrent);
        const needed = Number(metrics.expNeeded);
        if (Number.isFinite(current) && Number.isFinite(needed) && needed > 0) {
            const ratio = Math.min(Math.max(current / needed, 0), 1);
            const pct = ratio * 100;
            if (el.expProgressFill) el.expProgressFill.style.width = `${pct.toFixed(2)}%`;
            if (el.expProgressText) el.expProgressText.textContent = `${pct.toFixed(1)}%`;
            if (el.expEtaText) el.expEtaText.textContent = estimateLevelUpText(metrics);
        } else {
            if (el.expProgressFill) el.expProgressFill.style.width = '0%';
            if (el.expProgressText) el.expProgressText.textContent = '--';
            if (el.expEtaText) el.expEtaText.textContent = '预计升级时间：--';
        }
    } else {
        el.metricExp.textContent = '-';
        if (el.expProgressFill) el.expProgressFill.style.width = '0%';
        if (el.expProgressText) el.expProgressText.textContent = '--';
        if (el.expEtaText) el.expEtaText.textContent = '预计升级时间：--';
        resetExpEstimator();
    }

    if (el.sumHarvest) el.sumHarvest.textContent = formatNumber(metrics.harvest || 0);
    if (el.sumSteal) el.sumSteal.textContent = formatNumber(metrics.steal || 0);
    if (el.sumTask) el.sumTask.textContent = formatNumber(metrics.taskClaims || 0);
    if (el.sumSell) el.sumSell.textContent = formatNumber(metrics.soldItems || 0);
    if (el.btnLandsUpgradeAll) {
        el.btnLandsUpgradeAll.disabled = !isOnline;
        el.btnLandsUpgradeAll.title = isOnline
            ? '尝试对所有地块执行一次升级接口（单步升级）'
            : 'bot 未运行，无法执行升级地块请求';
    }
    updateOverviewBuy10hVisibility({ isOnline });
    if (el.btnBagUseAll) {
        el.btnBagUseAll.disabled = !isOnline;
        el.btnBagUseAll.title = isOnline
            ? '打开仓库弹窗'
            : 'bot 未运行，无法打开仓库';
    }
    if (state.mallBuyModalOpen) {
        refreshMallBuyModalForm();
    }
    if (!isOnline && state.bagItemsModalOpen) {
        setBagItemsModalError('bot 未运行，无法打开仓库');
    }
    renderCodeStatus();
}

function renderPlantOptions(options) {
    if (!el.preferredSeedId) return;
    const list = Array.isArray(options) ? options : [];
    state.plantOptions = list;

    const currentValue = state.config ? String(Number(state.config.preferredSeedId || 0)) : '0';
    el.preferredSeedId.innerHTML = '';

    const autoOption = document.createElement('option');
    autoOption.value = '0';
    autoOption.textContent = '自动选择（默认大萝卜）';
    el.preferredSeedId.appendChild(autoOption);

    for (const item of list) {
        const seedId = Number(item.seedId);
        if (!Number.isInteger(seedId) || seedId <= 0) continue;

        const option = document.createElement('option');
        option.value = String(seedId);
        const name = item.name || `seed-${seedId}`;
        const lv = Number.isInteger(Number(item.levelNeed)) ? Number(item.levelNeed) : 0;
        option.textContent = lv > 0 ? `${name} (Lv${lv})` : name;
        el.preferredSeedId.appendChild(option);
    }

    const hasCurrent = Array.from(el.preferredSeedId.options).some((opt) => opt.value === currentValue);
    if (!hasCurrent && currentValue !== '0') {
        const fallback = document.createElement('option');
        fallback.value = currentValue;
        fallback.textContent = `自定义种子(${currentValue})`;
        el.preferredSeedId.appendChild(fallback);
    }

    el.preferredSeedId.value = currentValue;
}

function renderConfig(config) {
    if (!config) return;
    state.config = config;

    el.platform.value = config.platform || 'wx';
    if (el.code) el.code.value = config.code || '';
    if (el.clientVersion) el.clientVersion.value = config.clientVersion || '';
    renderClientVersionHistoryOptions();
    setIntervalControl('farm', config.intervalSec ?? 1);
    applyPerformanceModeControl(config.performanceMode || 'standard', { syncFriendInterval: false });
    setIntervalControl('friend', config.friendIntervalSec ?? 1);
    if (el.fastHarvest) el.fastHarvest.checked = config.fastHarvest !== false;
    if (el.autoFertilize) el.autoFertilize.checked = config.autoFertilize !== false;
    updateOverviewBuy10hVisibility();
    if (el.friendActionSteal) el.friendActionSteal.checked = config.friendActionSteal !== false;
    const legacyCare = (config.friendActionWater !== false)
        || (config.friendActionWeed !== false)
        || (config.friendActionBug !== false);
    if (el.friendActionCare) el.friendActionCare.checked = config.friendActionCare !== undefined ? Boolean(config.friendActionCare) : legacyCare;
    if (el.friendActionPrank) el.friendActionPrank.checked = Boolean(config.friendActionPrank);
    setStealLevelThresholdControl(config.stealLevelThreshold);
    if (el.friendAutoDeleteNoStealEnabled) el.friendAutoDeleteNoStealEnabled.checked = Boolean(config.friendAutoDeleteNoStealEnabled);
    if (el.friendAutoDeleteNoStealDays) el.friendAutoDeleteNoStealDays.value = clampInt(config.friendAutoDeleteNoStealDays, 1, 3650, 7);
    if (el.friendActiveStart) el.friendActiveStart.value = config.friendActiveStart || '';
    if (el.friendActiveEnd) el.friendActiveEnd.value = config.friendActiveEnd || '';
    setFriendAllDayState(Boolean(config.friendActiveAllDay), { syncInputs: Boolean(config.friendActiveAllDay) });
    if (el.friendApplyActiveStart) el.friendApplyActiveStart.value = config.friendApplyActiveStart || '';
    if (el.friendApplyActiveEnd) el.friendApplyActiveEnd.value = config.friendApplyActiveEnd || '';
    setFriendApplyAllDayState(Boolean(config.friendApplyAllDay), { syncInputs: Boolean(config.friendApplyAllDay) });
    setIntervalControl('landRefresh', config.landRefreshIntervalSec ?? 5);
    if (el.preferredSeedId) {
        const val = String(Number(config.preferredSeedId || 0));
        const hasOption = Array.from(el.preferredSeedId.options).some((opt) => opt.value === val);
        if (!hasOption && val !== '0') {
            const fallback = document.createElement('option');
            fallback.value = val;
            fallback.textContent = `自定义种子(${val})`;
            el.preferredSeedId.appendChild(fallback);
        }
        el.preferredSeedId.value = val;
    }
    if (el.allowMulti) el.allowMulti.checked = Boolean(config.allowMulti);
    if (el.extraArgs) el.extraArgs.value = config.extraArgs || '';
    if (el.farmCalcUseAutoLands) {
        el.farmCalcUseAutoLands.checked = config.farmCalcUseAutoLands !== false;
    }
    if (el.farmCalcManualLands) {
        el.farmCalcManualLands.value = clampInt(config.farmCalcManualLands, 1, 500, 24);
    }
    renderCodeStatus();
    restartLandsPolling();
}

function renderNotificationSettings(settings) {
    const next = normalizeNotificationSettings(settings || {});
    state.notificationSettings = next;

    if (el.notifyEmailEnabled) el.notifyEmailEnabled.checked = next.notificationChannels.emailEnabled;
    if (el.notifyMailTo) el.notifyMailTo.value = next.notificationChannels.mailTo || '';
    if (el.notifySmtpHost) el.notifySmtpHost.value = next.notificationChannels.smtpHost || '';
    if (el.notifySmtpPort) el.notifySmtpPort.value = next.notificationChannels.smtpPort || 465;
    if (el.notifySmtpUser) el.notifySmtpUser.value = next.notificationChannels.smtpUser || '';
    if (el.notifySmtpPass) el.notifySmtpPass.value = next.notificationChannels.smtpPass || '';
    if (el.notifySmtpFromName) el.notifySmtpFromName.value = next.notificationChannels.smtpFromName || 'QQ Farm Bot';
    if (el.notifyServerChanEnabled) el.notifyServerChanEnabled.checked = next.notificationChannels.serverChanEnabled;
    if (el.notifyServerChanType) el.notifyServerChanType.value = next.notificationChannels.serverChanType || 'sc3';
    if (el.notifyServerChanKey) el.notifyServerChanKey.value = next.notificationChannels.serverChanKey || '';
    if (el.disconnectNotifyEmailEnabled) el.disconnectNotifyEmailEnabled.checked = next.disconnectNotify.emailEnabled;
    if (el.disconnectNotifyServerChanEnabled) el.disconnectNotifyServerChanEnabled.checked = next.disconnectNotify.serverChanEnabled;
    if (el.reportHourlyEnabled) el.reportHourlyEnabled.checked = next.reportNotify.hourlyEnabled;
    if (el.reportDailyEnabled) el.reportDailyEnabled.checked = next.reportNotify.dailyEnabled;
    if (el.reportNotifyEmailEnabled) el.reportNotifyEmailEnabled.checked = next.reportNotify.emailEnabled;
    if (el.reportNotifyServerChanEnabled) el.reportNotifyServerChanEnabled.checked = next.reportNotify.serverChanEnabled;
}

function collectNotificationSettingsPayload() {
    return normalizeNotificationSettings({
        notificationChannels: {
            emailEnabled: Boolean(el.notifyEmailEnabled && el.notifyEmailEnabled.checked),
            mailTo: el.notifyMailTo ? el.notifyMailTo.value.trim() : '',
            smtpHost: el.notifySmtpHost ? el.notifySmtpHost.value.trim() : '',
            smtpPort: el.notifySmtpPort ? Number(el.notifySmtpPort.value || 465) : 465,
            smtpUser: el.notifySmtpUser ? el.notifySmtpUser.value.trim() : '',
            smtpPass: el.notifySmtpPass ? el.notifySmtpPass.value.trim() : '',
            smtpFromName: el.notifySmtpFromName ? el.notifySmtpFromName.value.trim() : 'QQ Farm Bot',
            serverChanEnabled: Boolean(el.notifyServerChanEnabled && el.notifyServerChanEnabled.checked),
            serverChanType: el.notifyServerChanType ? el.notifyServerChanType.value : 'sc3',
            serverChanKey: el.notifyServerChanKey ? el.notifyServerChanKey.value.trim() : '',
        },
        disconnectNotify: {
            emailEnabled: Boolean(el.disconnectNotifyEmailEnabled && el.disconnectNotifyEmailEnabled.checked),
            serverChanEnabled: Boolean(el.disconnectNotifyServerChanEnabled && el.disconnectNotifyServerChanEnabled.checked),
        },
        reportNotify: {
            hourlyEnabled: Boolean(el.reportHourlyEnabled && el.reportHourlyEnabled.checked),
            dailyEnabled: Boolean(el.reportDailyEnabled && el.reportDailyEnabled.checked),
            emailEnabled: Boolean(el.reportNotifyEmailEnabled && el.reportNotifyEmailEnabled.checked),
            serverChanEnabled: Boolean(el.reportNotifyServerChanEnabled && el.reportNotifyServerChanEnabled.checked),
        },
    });
}

async function refreshNotificationSettings({ quiet = false } = {}) {
    try {
        const result = await apiGet('/api/notification-settings');
        renderNotificationSettings(result.settings || {});
    } catch (err) {
        if (!quiet) {
            el.actionHint.textContent = `通知设置加载失败: ${err.message}`;
        }
    }
}

async function saveNotificationSettings(options = {}) {
    const { silent = false } = options || {};
    const payload = collectNotificationSettingsPayload();
    const result = await apiPost('/api/notification-settings', payload);
    renderNotificationSettings(result.settings || payload);
    if (!silent) {
        el.actionHint.textContent = result.message || '当前账号通知设置已保存';
    }
}

function formatNotificationTestResult(result, successText, skippedPrefix) {
    if (!result || typeof result !== 'object') return successText;
    if (result.skipped) return `${skippedPrefix}: ${result.reason || '已跳过'}`;
    if (Array.isArray(result.results) && result.results.length > 0) {
        const details = result.results.map((item) => {
            if (item.ok) return `${item.channel} 成功`;
            return `${item.channel} 失败${item.error ? `(${item.error})` : ''}`;
        }).join('，');
        return `${successText}：${details}`;
    }
    return successText;
}

async function testDisconnectNotification() {
    await saveNotificationSettings({ silent: true });
    const result = await apiPost('/api/notification-settings/test-disconnect', { reason: 'WEBUI 手动测试掉线提醒' });
    el.actionHint.textContent = formatNotificationTestResult(result && result.result, '已触发测试掉线提醒', '掉线提醒未发送');
}

async function testReportNotification(type) {
    await saveNotificationSettings({ silent: true });
    const normalizedType = String(type || '').trim().toLowerCase() === 'daily' ? 'daily' : 'hourly';
    const result = await apiPost('/api/notification-settings/test-report', { type: normalizedType });
    el.actionHint.textContent = formatNotificationTestResult(
        result && result.result,
        `已触发测试${normalizedType === 'daily' ? '每日' : '小时'}汇报`,
        `${normalizedType === 'daily' ? '每日' : '小时'}汇报未发送`
    );
}

async function loadInitial() {
    const requestList = [
        apiGet('/api/config'),
        apiGet('/api/runtime'),
        apiGet('/api/logs?limit=500'),
        apiGet('/api/notification-settings').catch(() => ({ ok: false, settings: null })),
        apiGet('/api/stats/overview').catch(() => ({ ok: false, summary24h: null, hourly: [], daily: [], source: { note: '收益统计加载失败' } })),
        apiGet('/api/plant-options').catch(() => ({ options: [] })),
        apiGet('/api/lands').catch(() => ({ ok: false, hasSnapshot: false, message: '地块快照读取失败', snapshot: null })),
        apiGet(buildFriendInsightsPath()).catch(() => ({ ok: false, friends: [], overview: null, source: { note: '好友统计加载失败' } })),
    ];
    if (el.shareContent) {
        requestList.push(apiGet('/api/share').catch(() => ({ content: '' })));
    }
    const [configRes, runtimeRes, logsRes, notificationRes, overviewStatsRes, plantRes, landsRes, friendsRes, shareRes] = await Promise.all(requestList);

    renderPlantOptions(plantRes.options || []);
    renderConfig(configRes);
    renderRuntime(runtimeRes);
    renderNotificationSettings(notificationRes && notificationRes.settings ? notificationRes.settings : null);
    renderOverviewStats(overviewStatsRes);

    state.logs = logsRes.logs || [];
    renderLogs();
    renderLands(landsRes);
    state.friendInsights = friendsRes;
    renderFriendInsights(friendsRes);
    setFriendsHint((friendsRes && friendsRes.source && friendsRes.source.note) || '好友统计已加载');

    if (el.shareContent && shareRes) {
        el.shareContent.value = shareRes.content || '';
    }
    el.actionHint.textContent = '已加载最新状态';
    await refreshFarmCalcRecommendation({ quiet: true });
}

async function saveConfig({ auto = false, hint = '' } = {}) {
    const payload = collectConfigPayload();
    const result = await apiPost('/api/config', payload);
    renderConfig(result.config);
    el.actionHint.textContent = hint || result.message || (auto ? '配置已自动保存' : '配置已保存，下一轮自动生效');
    scheduleFarmCalcRefresh(150);
}

async function saveShare() {
    if (!el.shareContent) return;
    const content = el.shareContent.value;
    const result = await apiPost('/api/share', { content });
    el.actionHint.textContent = result.message || 'share.txt 已保存';
}

async function runBotAction(url, hint) {
    const result = await apiPost(url, {});
    if (result.runtime) renderRuntime(result.runtime);
    el.actionHint.textContent = hint;
}

async function requestUpgradeAllLandsOnce() {
    const result = await apiPost('/api/lands/upgrade-all-once', {});
    el.actionHint.textContent = result.message || '已请求尝试升级所有地块一次';
    if (el.landsHint) {
        el.landsHint.textContent = '已发送升级地块请求，等待 bot 下一轮巡田执行。';
    }
    return result;
}

async function requestMallDailyClaimOnce() {
    const result = await apiPost('/api/mall/claim-daily-once', {});
    el.actionHint.textContent = result.message || '已请求领取每日福利一次';
    return result;
}

async function requestMallBuy10hFertOnce(count) {
    const safeCount = clampInt(count, 1, MALL_FERT_BUY_MAX_COUNT, 1);
    const result = await apiPost('/api/mall/buy-fert10h-once', { count: safeCount });
    el.actionHint.textContent = result.message || `已请求购买10小时化肥 x${safeCount}（完成后会自动使用仓库道具）`;
    return result;
}

async function requestUseAllBagItemsOnce() {
    const result = await apiPost('/api/items/use-all-once', {});
    el.actionHint.textContent = result.message || '已请求一键使用仓库道具';
    return result;
}

async function requestBagSnapshotOnce() {
    const result = await apiPost('/api/items/refresh-bag-once', {});
    return result;
}

async function fetchBagSnapshot() {
    return apiGet('/api/items/bag-snapshot');
}

async function requestUseSelectedBagItemsOnce(items) {
    const list = Array.isArray(items) ? items : [];
    const result = await apiPost('/api/items/use-selected-once', { items: list });
    el.actionHint.textContent = result.message || `已请求打开勾选道具 ${list.length} 项`;
    return result;
}

function pushLog(entry) {
    state.logs.push(entry);
    if (state.logs.length > 2000) {
        state.logs.splice(0, state.logs.length - 2000);
    }
    const text = String(entry && entry.text ? entry.text : '');
    if (isOverviewStatsRelevantLine(text)) {
        scheduleOverviewStatsRefresh(1400);
    }
    if (state.mallBuyPending && /10小时化肥购买失败/.test(text)) {
        resetMallBuyPending();
        if (state.runtime && el.metricDiamond) {
            el.metricDiamond.textContent = formatNumber(getCurrentDiamondValue(state.runtime));
        }
    }
    renderLogs();
}

function updateLogAliveIndicator() {
    if (!el.logAliveBadge) return;
    const now = Date.now();
    const latestEventAt = Math.max(
        state.health.lastPingAt || 0,
        state.health.lastRuntimeAt || 0,
        state.health.lastSseEventAt || 0
    );
    const isFresh = latestEventAt > 0 && (now - latestEventAt) < 35000;

    let mode = 'down';
    let title = '后台连接状态：连接中断';
    if (state.health.sseConnected && isFresh) {
        if (state.health.runtimeRunning) {
            mode = 'alive';
            title = '后台连接状态：运行中';
        } else {
            mode = 'idle';
            title = '后台连接状态：连接正常，未运行';
        }
    }

    el.logAliveBadge.classList.remove('alive', 'idle', 'down');
    el.logAliveBadge.classList.add(mode);
    el.logAliveBadge.title = title;
}

function markSseActivity({ runtimeRunning } = {}) {
    state.health.sseConnected = true;
    state.health.lastSseEventAt = Date.now();
    if (typeof runtimeRunning === 'boolean') {
        state.health.runtimeRunning = runtimeRunning;
    }
}

function bindEvents() {
    bindMainTabs();
    bindIntervalControl('farm');
    bindIntervalControl('friend');
    bindIntervalControl('landRefresh');
    bindStealLevelThresholdControl();

    if (el.performanceMode) {
        el.performanceMode.value = state.performanceMode;
        el.performanceMode.addEventListener('change', () => {
            const preset = getPerformancePreset(el.performanceMode.value);
            applyPerformanceModeControl(preset.mode, { syncFriendInterval: true, syncFarmInterval: true, syncFriendPatrol: true });
            scheduleConfigAutoSave(0, `已切换为${preset.label}`);
        });
    }

    if (el.friendsSortKey) {
        el.friendsSortKey.value = state.friendSort.key;
        el.friendsSortKey.addEventListener('change', () => {
            state.friendSort.key = normalizeFriendSortKey(el.friendsSortKey.value);
            el.friendsSortKey.value = state.friendSort.key;
            state.friendPage = 1;
            void refreshFriendInsights({ quiet: true });
        });
    }

    if (el.friendsSortOrder) {
        el.friendsSortOrder.value = state.friendSort.order;
        el.friendsSortOrder.addEventListener('change', () => {
            state.friendSort.order = normalizeFriendSortOrder(el.friendsSortOrder.value);
            el.friendsSortOrder.value = state.friendSort.order;
            state.friendPage = 1;
            void refreshFriendInsights({ quiet: true });
        });
    }

    if (el.friendsNameSearch) {
        el.friendsNameSearch.value = state.friendFilters.search;
        el.friendsNameSearch.addEventListener('input', () => {
            state.friendFilters.search = String(el.friendsNameSearch.value || '').trim();
            rerenderFriendInsightsWithCurrentFilters({ resetPage: true });
        });
    }

    if (el.friendsOnlyFailed) {
        el.friendsOnlyFailed.checked = state.friendFilters.onlyFailed;
        el.friendsOnlyFailed.addEventListener('change', () => {
            state.friendFilters.onlyFailed = Boolean(el.friendsOnlyFailed.checked);
            rerenderFriendInsightsWithCurrentFilters({ resetPage: true });
        });
    }

    if (el.friendsOnlyEffective) {
        el.friendsOnlyEffective.checked = state.friendFilters.onlyEffective;
        el.friendsOnlyEffective.addEventListener('change', () => {
            state.friendFilters.onlyEffective = Boolean(el.friendsOnlyEffective.checked);
            rerenderFriendInsightsWithCurrentFilters({ resetPage: true });
        });
    }

    if (el.btnFriendsRefresh) {
        el.btnFriendsRefresh.addEventListener('click', () => {
            void refreshFriendInsights();
        });
    }

    if (el.btnFriendsPrev) {
        el.btnFriendsPrev.addEventListener('click', () => {
            state.friendPage = Math.max(1, state.friendPage - 1);
            rerenderFriendInsightsWithCurrentFilters();
        });
    }

    if (el.btnFriendsNext) {
        el.btnFriendsNext.addEventListener('click', () => {
            state.friendPage += 1;
            rerenderFriendInsightsWithCurrentFilters();
        });
    }

    if (el.friendsList) {
        el.friendsList.addEventListener('click', (ev) => {
            const button = ev.target.closest('.friend-delete-btn');
            if (!button) return;
            const gid = Number(button.dataset.friendGid || 0);
            const name = String(button.dataset.friendName || '').trim() || `GID:${gid}`;
            if (!Number.isFinite(gid) || gid <= 0) {
                el.actionHint.textContent = '该好友缺少 gid，无法删除';
                return;
            }
            if (!window.confirm(`确认删除好友“${name}”吗？`)) {
                return;
            }

            button.disabled = true;
            void (async () => {
                try {
                    const result = await requestFriendDelete(gid, name);
                    setFriendsHint(result.message || `已请求删除好友 ${name}`);
                } catch (err) {
                    const message = `删除好友请求失败: ${err.message}`;
                    setFriendsHint(message, true);
                    el.actionHint.textContent = message;
                } finally {
                    button.disabled = false;
                }
            })();
        });

        el.friendsList.addEventListener('change', (ev) => {
            const input = ev.target.closest('.friend-steal-toggle');
            if (!input) return;
            const stealKey = String(input.dataset.stealKey || '').trim();
            if (!stealKey) return;

            const updateLabel = () => {
                const label = input.closest('.friend-steal-switch');
                const text = label ? label.querySelector('span') : null;
                if (text) text.textContent = input.checked ? '偷菜开' : '偷菜关';
            };
            const nextChecked = Boolean(input.checked);
            updateLabel();

            input.disabled = true;
            void (async () => {
                try {
                    await saveFriendStealSwitch(stealKey, nextChecked);
                } catch (err) {
                    input.checked = !nextChecked;
                    updateLabel();
                    el.actionHint.textContent = `更新偷菜开关失败: ${err.message}`;
                } finally {
                    input.disabled = false;
                }
            })();
        });
    }

    el.configForm.addEventListener('submit', async (ev) => {
        ev.preventDefault();
        scheduleConfigAutoSave(0);
    });

    if (el.btnCodePrompt) {
        el.btnCodePrompt.addEventListener('click', () => {
            promptAndUpdateCode();
        });
    }

    if (el.btnCodeModalClose) {
        el.btnCodeModalClose.addEventListener('click', () => closeCodeModal());
    }
    if (el.btnCodeModalCancel) {
        el.btnCodeModalCancel.addEventListener('click', () => closeCodeModal());
    }
    if (el.btnCodeModalConfirm) {
        el.btnCodeModalConfirm.addEventListener('click', () => submitCodeModal());
    }
    if (el.btnCodeAutoStart) {
        el.btnCodeAutoStart.addEventListener('click', () => {
            void startCodeAutoCapture();
        });
    }
    if (el.btnCodeAutoCert) {
        el.btnCodeAutoCert.addEventListener('click', () => {
            window.open(AUTO_CODE_CERT_URL, '_blank', 'noopener,noreferrer');
        });
    }
    if (el.btnCodeAutoCopyHost) {
        el.btnCodeAutoCopyHost.addEventListener('click', async () => {
            try {
                await copyText(AUTO_CODE_PROXY_HOST, '代理服务器已复制');
            } catch (err) {
                setCodeModalError(`复制失败: ${err.message}`);
            }
        });
    }
    if (el.btnCodeAutoCopyPort) {
        el.btnCodeAutoCopyPort.addEventListener('click', async () => {
            try {
                await copyText(AUTO_CODE_PROXY_PORT, '代理端口已复制');
            } catch (err) {
                setCodeModalError(`复制失败: ${err.message}`);
            }
        });
    }
    if (el.codeModalBackdrop) {
        el.codeModalBackdrop.addEventListener('click', () => closeCodeModal());
    }
    if (el.codeModalInput) {
        el.codeModalInput.addEventListener('input', () => {
            setCodeModalError('');
            updateCodeModalConfirmState();
        });
        el.codeModalInput.addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter') {
                ev.preventDefault();
                submitCodeModal();
            } else if (ev.key === 'Escape') {
                ev.preventDefault();
                closeCodeModal();
            }
        });
    }
    if (el.codeModalVersionInput) {
        el.codeModalVersionInput.addEventListener('input', () => {
            setCodeModalError('');
            updateCodeModalConfirmState();
        });
        el.codeModalVersionInput.addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter') {
                ev.preventDefault();
                submitCodeModal();
            } else if (ev.key === 'Escape') {
                ev.preventDefault();
                closeCodeModal();
            }
        });
    }
    if (el.codeModal) {
        el.codeModal.addEventListener('keydown', (ev) => {
            if (ev.key === 'Escape') {
                ev.preventDefault();
                closeCodeModal();
            }
        });
    }

    if (el.codeStatusChip) {
        const tryPromptFromStatus = () => {
            const view = getCodeStatusView();
            if (!view.clickable) return;
            promptAndUpdateCode();
        };
        el.codeStatusChip.addEventListener('click', tryPromptFromStatus);
        el.codeStatusChip.addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter' || ev.key === ' ') {
                ev.preventDefault();
                tryPromptFromStatus();
            }
        });
    }

    if (el.btnFriendActiveClear) {
        el.btnFriendActiveClear.addEventListener('click', () => {
            if (el.friendActiveStart) el.friendActiveStart.value = '';
            if (el.friendActiveEnd) el.friendActiveEnd.value = '';
            setFriendAllDayState(false, { syncInputs: false });
            scheduleConfigAutoSave(0, '好友巡查时段已清空');
        });
    }

    if (el.btnFriendApplyClear) {
        el.btnFriendApplyClear.addEventListener('click', () => {
            if (el.friendApplyActiveStart) el.friendApplyActiveStart.value = '';
            if (el.friendApplyActiveEnd) el.friendApplyActiveEnd.value = '';
            setFriendApplyAllDayState(false, { syncInputs: false });
            scheduleConfigAutoSave(0, '好友申请同意时段已清空');
        });
    }

    if (el.btnSaveNotificationSettings) {
        el.btnSaveNotificationSettings.addEventListener('click', async () => {
            if (state.notificationSettingsSaving) return;
            state.notificationSettingsSaving = true;
            try {
                await saveNotificationSettings();
            } catch (err) {
                el.actionHint.textContent = `保存通知设置失败: ${err.message}`;
            } finally {
                state.notificationSettingsSaving = false;
            }
        });
    }

    if (el.btnTestDisconnectNotification) {
        el.btnTestDisconnectNotification.addEventListener('click', async () => {
            try {
                await testDisconnectNotification();
            } catch (err) {
                el.actionHint.textContent = `测试掉线提醒失败: ${err.message}`;
            }
        });
    }

    if (el.btnTestHourlyReport) {
        el.btnTestHourlyReport.addEventListener('click', async () => {
            try {
                await testReportNotification('hourly');
            } catch (err) {
                el.actionHint.textContent = `测试小时汇报失败: ${err.message}`;
            }
        });
    }

    if (el.btnTestDailyReport) {
        el.btnTestDailyReport.addEventListener('click', async () => {
            try {
                await testReportNotification('daily');
            } catch (err) {
                el.actionHint.textContent = `测试每日汇报失败: ${err.message}`;
            }
        });
    }

    if (el.friendActiveAllDay) {
        el.friendActiveAllDay.addEventListener('change', () => {
            setFriendAllDayState(el.friendActiveAllDay.checked, { syncInputs: true });
            scheduleConfigAutoSave(0, el.friendActiveAllDay.checked ? '已开启全天巡查' : '已关闭全天巡查');
        });
    }

    if (el.friendApplyAllDay) {
        el.friendApplyAllDay.addEventListener('change', () => {
            setFriendApplyAllDayState(el.friendApplyAllDay.checked, { syncInputs: true });
            scheduleConfigAutoSave(0, el.friendApplyAllDay.checked ? '已开启全天自动同意' : '已关闭全天自动同意');
        });
    }

    const autoSaveChangeEls = [
        el.fastHarvest,
        el.autoFertilize,
        el.friendActionSteal,
        el.friendActionCare,
        el.friendActionPrank,
        el.friendAutoDeleteNoStealEnabled,
        el.friendAutoDeleteNoStealDays,
        el.friendActiveStart,
        el.friendActiveEnd,
        el.friendApplyActiveStart,
        el.friendApplyActiveEnd,
        el.preferredSeedId,
        el.allowMulti,
        el.extraArgs,
        el.farmCalcUseAutoLands,
        el.farmCalcManualLands,
    ].filter(Boolean);

    for (const node of autoSaveChangeEls) {
        const isTextLike = ['INPUT', 'TEXTAREA'].includes(node.tagName) && node.type !== 'checkbox' && node.type !== 'time';
        node.addEventListener('input', () => {
            if (node === el.farmCalcManualLands) getFarmCalcManualLandsValue();
            if (node === el.farmCalcManualLands || node === el.farmCalcUseAutoLands) {
                scheduleFarmCalcRefresh(180);
            }
            if (node === el.friendActiveStart || node === el.friendActiveEnd) {
                if (el.friendActiveAllDay && el.friendActiveAllDay.checked) {
                    setFriendAllDayState(false, { syncInputs: false });
                }
            }
            if (node === el.friendApplyActiveStart || node === el.friendApplyActiveEnd) {
                if (el.friendApplyAllDay && el.friendApplyAllDay.checked) {
                    setFriendApplyAllDayState(false, { syncInputs: false });
                }
            }
            if (node === el.autoFertilize) {
                updateOverviewBuy10hVisibility();
            }
            scheduleConfigAutoSave(isTextLike ? 260 : 120);
        });
        node.addEventListener('change', () => {
            if (node === el.farmCalcManualLands) getFarmCalcManualLandsValue();
            if (node === el.farmCalcManualLands || node === el.farmCalcUseAutoLands) {
                scheduleFarmCalcRefresh(180);
            }
            if (node === el.friendActiveStart || node === el.friendActiveEnd) {
                if (el.friendActiveAllDay && el.friendActiveAllDay.checked) {
                    setFriendAllDayState(false, { syncInputs: false });
                }
            }
            if (node === el.friendApplyActiveStart || node === el.friendApplyActiveEnd) {
                if (el.friendApplyAllDay && el.friendApplyAllDay.checked) {
                    setFriendApplyAllDayState(false, { syncInputs: false });
                }
            }
            if (node === el.autoFertilize) {
                updateOverviewBuy10hVisibility();
            }
            scheduleConfigAutoSave(0);
        });
    }

    if (el.btnToggle) {
        el.btnToggle.addEventListener('click', async () => {
            if (el.btnToggle.disabled) return;
            const running = Boolean(state.runtime && state.runtime.running);
            const url = running ? '/api/bot/stop' : '/api/bot/start';
            const hint = running ? '停止命令已发送' : '启动命令已发送';
            try {
                el.btnToggle.disabled = true;
                await runBotAction(url, hint);
            } catch (err) {
                el.actionHint.textContent = `${running ? '停止' : '启动'}失败: ${err.message}`;
            } finally {
                el.btnToggle.disabled = false;
            }
        });
    }

    if (el.btnLandsUpgradeAll) {
        el.btnLandsUpgradeAll.addEventListener('click', async () => {
            if (el.btnLandsUpgradeAll.disabled) return;
            try {
                el.btnLandsUpgradeAll.disabled = true;
                await requestUpgradeAllLandsOnce();
            } catch (err) {
                el.actionHint.textContent = `请求升级地块失败: ${err.message}`;
            } finally {
                const running = Boolean(state.runtime && state.runtime.running);
                el.btnLandsUpgradeAll.disabled = !running;
            }
        });
    }

    if (el.btnOverviewBuy10h) {
        el.btnOverviewBuy10h.addEventListener('click', () => {
            if (el.btnOverviewBuy10h.disabled) return;
            openMallBuyModal();
        });
    }

    if (el.mallBuyModalBackdrop) {
        el.mallBuyModalBackdrop.addEventListener('click', () => closeMallBuyModal());
    }
    if (el.btnMallBuyModalClose) {
        el.btnMallBuyModalClose.addEventListener('click', () => closeMallBuyModal());
    }
    if (el.btnMallBuyModalCancel) {
        el.btnMallBuyModalCancel.addEventListener('click', () => closeMallBuyModal());
    }
    if (el.mallBuyCountInput) {
        const normalizeMallBuyCount = () => {
            const value = clampInt(el.mallBuyCountInput.value, 0, MALL_FERT_BUY_MAX_COUNT, 1);
            el.mallBuyCountInput.value = String(value);
            refreshMallBuyModalForm();
        };
        el.mallBuyCountInput.addEventListener('input', normalizeMallBuyCount);
        el.mallBuyCountInput.addEventListener('change', normalizeMallBuyCount);
        el.mallBuyCountInput.addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter') {
                ev.preventDefault();
                void submitMallBuyModal();
            } else if (ev.key === 'Escape') {
                ev.preventDefault();
                closeMallBuyModal();
            }
        });
    }
    if (el.btnMallBuyModalConfirm) {
        el.btnMallBuyModalConfirm.addEventListener('click', () => {
            void submitMallBuyModal();
        });
    }
    if (el.mallBuyModal) {
        el.mallBuyModal.addEventListener('keydown', (ev) => {
            if (ev.key === 'Escape') {
                ev.preventDefault();
                closeMallBuyModal();
            }
        });
    }

    if (el.btnBagUseAll) {
        el.btnBagUseAll.addEventListener('click', () => {
            if (el.btnBagUseAll.disabled) return;
            openBagItemsModal();
        });
    }
    if (el.bagItemsModalBackdrop) {
        el.bagItemsModalBackdrop.addEventListener('click', () => closeBagItemsModal());
    }
    if (el.btnBagItemsModalClose) {
        el.btnBagItemsModalClose.addEventListener('click', () => closeBagItemsModal());
    }
    if (el.btnBagItemsModalCancel) {
        el.btnBagItemsModalCancel.addEventListener('click', () => closeBagItemsModal());
    }
    if (el.btnBagItemsRefresh) {
        el.btnBagItemsRefresh.addEventListener('click', () => {
            void refreshBagItemsModalSnapshot({ forceFresh: true, preserveSelection: true });
        });
    }
    const bindWarehouseTab = (node, key) => {
        if (!node) return;
        node.addEventListener('click', () => {
            if (state.bagItemsLoading) return;
            setWarehouseTab(key);
        });
    };
    bindWarehouseTab(el.btnWarehouseTabFruits, 'fruits');
    bindWarehouseTab(el.btnWarehouseTabSuperFruits, 'superfruits');
    bindWarehouseTab(el.btnWarehouseTabSeeds, 'seeds');
    bindWarehouseTab(el.btnWarehouseTabProps, 'props');
    if (el.btnBagItemsSelectAll) {
        el.btnBagItemsSelectAll.addEventListener('click', () => {
            const items = getWarehouseSnapshotGroups(state.bagItemsSnapshot).props;
            state.bagItemsSelection = new Set(items.map((item) => bagItemKey(item)));
            renderBagItemsModalList();
            setBagItemsModalError('');
        });
    }
    if (el.btnBagItemsClear) {
        el.btnBagItemsClear.addEventListener('click', () => {
            state.bagItemsSelection = new Set();
            renderBagItemsModalList();
        });
    }
    if (el.btnBagItemsModalConfirm) {
        el.btnBagItemsModalConfirm.addEventListener('click', () => {
            void submitBagItemsModal();
        });
    }
    if (el.bagItemsModal) {
        el.bagItemsModal.addEventListener('keydown', (ev) => {
            if (ev.key === 'Escape') {
                ev.preventDefault();
                closeBagItemsModal();
            }
        });
    }

    if (el.shareForm) {
        el.shareForm.addEventListener('submit', async (ev) => {
            ev.preventDefault();
            try {
                await saveShare();
            } catch (err) {
                el.actionHint.textContent = `保存 share 失败: ${err.message}`;
            }
        });
    }

    if (el.btnFarmCalcRefresh) {
        el.btnFarmCalcRefresh.addEventListener('click', async () => {
            try {
                el.btnFarmCalcRefresh.disabled = true;
                await refreshFarmCalcRecommendation();
            } finally {
                el.btnFarmCalcRefresh.disabled = false;
            }
        });
    }

    if (el.btnFarmCalcApplyBest) {
        el.btnFarmCalcApplyBest.addEventListener('click', async () => {
            try {
                el.btnFarmCalcApplyBest.disabled = true;
                await applyBestFarmCalcSeed();
            } catch (err) {
                el.actionHint.textContent = `应用推荐失败: ${err.message}`;
            } finally {
                el.btnFarmCalcApplyBest.disabled = false;
            }
        });
    }

    el.filterRow.addEventListener('click', (ev) => {
        const btn = ev.target.closest('.filter-chip');
        if (!btn) return;
        state.activeTag = btn.dataset.tag || 'ALL';
        for (const chip of el.filterRow.querySelectorAll('.filter-chip')) {
            chip.classList.toggle('active', chip === btn);
        }
        renderLogs();
    });

    el.logSearch.addEventListener('input', () => {
        state.search = el.logSearch.value.trim();
        renderLogs();
    });

}

function connectSSE() {
    const es = new EventSource('/api/events');

    es.addEventListener('runtime', (event) => {
        try {
            const runtime = JSON.parse(event.data);
            markSseActivity({ runtimeRunning: Boolean(runtime && runtime.running) });
            state.health.lastRuntimeAt = Date.now();
            renderRuntime(runtime);
            updateLogAliveIndicator();
        } catch (err) {
            // ignore
        }
    });

    es.addEventListener('metrics', (event) => {
        try {
            const metrics = JSON.parse(event.data);
            markSseActivity();
            if (state.runtime) {
                renderRuntime({ ...state.runtime, metrics });
            }
            updateLogAliveIndicator();
        } catch (err) {
            // ignore
        }
    });

    es.addEventListener('config', (event) => {
        try {
            markSseActivity();
            renderConfig(JSON.parse(event.data));
            scheduleFarmCalcRefresh(180);
            updateLogAliveIndicator();
        } catch (err) {
            // ignore
        }
    });

    es.addEventListener('log', (event) => {
        try {
            markSseActivity();
            const entry = JSON.parse(event.data);
            pushLog(entry);
            updateLogAliveIndicator();
        } catch (err) {
            // ignore
        }
    });

    es.addEventListener('ping', () => {
        markSseActivity();
        state.health.lastPingAt = Date.now();
        updateLogAliveIndicator();
    });

    es.onerror = () => {
        state.health.sseConnected = false;
        updateLogAliveIndicator();
        el.actionHint.textContent = '实时连接中断，正在自动重连...';
    };

    es.onopen = () => {
        markSseActivity();
        state.health.lastPingAt = Date.now();
        updateLogAliveIndicator();
        el.actionHint.textContent = '实时连接已建立';
    };
}

async function fetchAdminStatus() {
    try {
        const data = await apiGet('/api/admin/status');
        const loggedInEl = document.getElementById('statLoggedIn');
        const runningEl = document.getElementById('statRunning');
        if (loggedInEl) loggedInEl.textContent = data.loggedIn;
        if (runningEl) runningEl.textContent = data.running;
    } catch (e) { }
}

async function handleLogout() {
    try {
        await fetch('/api/auth/logout', { method: 'POST' });
    } catch (e) { }
    window.location.href = '/login';
}

async function main() {
    // Auth check: redirect to login if not authenticated
    try {
        const meRes = await fetch('/api/auth/me');
        if (!meRes.ok) {
            window.location.href = '/login';
            return;
        }
        const me = await meRes.json();
        const nameEl = document.getElementById('userBarName');
        if (nameEl) nameEl.textContent = me.username;
    } catch (e) {
        window.location.href = '/login';
        return;
    }

    bindEvents();
    startUptimeTicker();
    startLandsTicker();
    try {
        await loadInitial();
        if (state.runtime) {
            state.health.runtimeRunning = Boolean(state.runtime.running);
            state.health.lastRuntimeAt = Date.now();
        }
        updateLogAliveIndicator();
    } catch (err) {
        el.actionHint.textContent = `初始化失败: ${err.message}`;
    }
    setInterval(updateLogAliveIndicator, 5000);
    fetchAdminStatus();
    setInterval(fetchAdminStatus, 30000);
    restartLandsPolling();
    startFriendInsightsPolling();
    connectSSE();
}

main();

