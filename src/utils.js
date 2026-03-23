/**
 * Shared utility helpers.
 */

const Long = require('long');
const { RUNTIME_HINT_MASK, RUNTIME_HINT_DATA } = require('./config');

let serverTimeMs = 0;
let localTimeAtSync = 0;

function toLong(val) {
    return Long.fromNumber(val);
}

function toNum(val) {
    if (Long.isLong(val)) return val.toNumber();
    return val || 0;
}

function now() {
    return new Date().toLocaleTimeString();
}

function getServerTimeSec() {
    if (!serverTimeMs) return Math.floor(Date.now() / 1000);
    const elapsed = Date.now() - localTimeAtSync;
    return Math.floor((serverTimeMs + elapsed) / 1000);
}

function syncServerTime(ms) {
    serverTimeMs = ms;
    localTimeAtSync = Date.now();
}

/**
 * Normalize timestamp value to seconds.
 * If value looks like milliseconds, divide by 1000.
 */
function toTimeSec(val) {
    const n = toNum(val);
    if (n <= 0) return 0;
    if (n > 1e12) return Math.floor(n / 1000);
    return n;
}

const MOJIBAKE_REPLACEMENTS = [
    [/璇锋眰瓒呮椂/g, '请求超时'],
    [/鐠囬攱鐪扮搾鍛/g, '请求超时'],
    [/杩炴帴鍙兘宸叉柇寮€/g, '连接可能已断开'],
    [/灏濊瘯閲嶈繛/g, '尝试重连'],
    [/鏃犲搷搴\??/g, '无响应'],
    [/蹇冭烦/g, '心跳'],
];

const LATIN1_MOJIBAKE_RE = /[À-ÿ]/g;
const CJK_MOJIBAKE_RE = /[闂鍙鐎閺閻缁娑鎼妫鏌鈧銆鍩鍛鍚鍓鍔鍗鍘妯銈銉绱绮璇]/g;
const MOJIBAKE_TOKEN_RE = /(闂|鍊|閹|缁|鎼|璇|杩|蹇|灏濊|鐎|鍛|鍓|鍔|鍗|崐|妯|銈)/g;
const REPLACEMENT_CHAR_RE = /�/g;

function applyMojibakeReplacements(text) {
    let out = String(text == null ? '' : text);
    for (const [pattern, value] of MOJIBAKE_REPLACEMENTS) {
        out = out.replace(pattern, value);
    }
    return out;
}

function countMatch(text, re) {
    const found = text.match(re);
    return found ? found.length : 0;
}

function isLikelyMojibake(text) {
    const input = String(text || '');
    if (!input) return false;

    const latin1Count = countMatch(input, LATIN1_MOJIBAKE_RE);
    const cjkCount = countMatch(input, CJK_MOJIBAKE_RE);
    const tokenCount = countMatch(input, MOJIBAKE_TOKEN_RE);
    const replacementCount = countMatch(input, REPLACEMENT_CHAR_RE);
    const badCount = latin1Count + cjkCount + replacementCount * 2;
    const ratio = badCount / Math.max(1, input.length);

    if (input.length >= 60 && badCount >= 12 && ratio >= 0.2) return true;
    if (input.length >= 180 && badCount >= 20 && ratio >= 0.12) return true;
    if (input.length >= 800 && badCount >= 24) return true;
    if (input.length >= 20 && cjkCount >= 6 && ratio >= 0.18) return true;
    if (input.length >= 10 && cjkCount >= 4 && ratio >= 0.28) return true;
    if (input.length >= 10 && tokenCount >= 3) return true;
    return false;
}

function truncateText(text, maxLen, suffix) {
    if (text.length <= maxLen) return text;
    return `${text.slice(0, Math.max(0, maxLen))}${suffix}`;
}

function summarizeKnownIssue(text) {
    const input = String(text || '');
    if (!input) return '';
    if (input.includes('请求超时')) return '请求超时';
    if (input.includes('连接可能已断开')) return '连接可能已断开';
    if (input.includes('尝试重连')) return '尝试重连';
    if (input.includes('无响应')) return '无响应';
    if (input.includes('心跳')) return '心跳异常';
    return '';
}

function sanitizeLogText(value) {
    let text = String(value == null ? '' : value);
    text = text.replace(/\r?\n+/g, ' ').replace(/\s+/g, ' ').trim();
    text = applyMojibakeReplacements(text);

    if (!text) return '';
    if (isLikelyMojibake(text)) {
        const knownIssue = summarizeKnownIssue(text);
        if (knownIssue) return `${knownIssue}（原始返回疑似乱码）`;
        return '返回内容疑似乱码，已省略';
    }
    if (text.length > 1200) {
        return truncateText(text, 600, '...(日志过长，已截断)');
    }
    return text;
}

function sanitizeLogTag(tag) {
    let text = String(tag == null ? '' : tag).trim();
    text = applyMojibakeReplacements(text).replace(/\s+/g, '');
    if (!text) return 'LOG';
    if (text === '心跳') return 'HB';
    if (countMatch(text, MOJIBAKE_TOKEN_RE) >= 1) return 'LOG';
    if (isLikelyMojibake(text)) return 'LOG';
    return text.length > 16 ? text.slice(0, 16) : text;
}

function log(tag, msg) {
    console.log(`[${now()}] [${sanitizeLogTag(tag)}] ${sanitizeLogText(msg)}`);
}

function logWarn(tag, msg) {
    console.log(`[${now()}] [${sanitizeLogTag(tag)}] [WARN] ${sanitizeLogText(msg)}`);
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

let hintPrinted = false;

function decodeRuntimeHint() {
    return String.fromCharCode(...RUNTIME_HINT_DATA.map((n) => n ^ RUNTIME_HINT_MASK));
}

/**
 * Print runtime hint:
 * - force=true always prints (used at startup)
 * - otherwise prints at low probability.
 */
function emitRuntimeHint(force = false) {
    if (!force) {
        if (Math.random() > 0.033) return;
        if (hintPrinted && Math.random() > 0.2) return;
    }
    log('声明', decodeRuntimeHint());
    hintPrinted = true;
}

module.exports = {
    toLong,
    toNum,
    now,
    getServerTimeSec,
    syncServerTime,
    toTimeSec,
    log,
    logWarn,
    sleep,
    emitRuntimeHint,
    sanitizeLogText,
    sanitizeLogTag,
};
