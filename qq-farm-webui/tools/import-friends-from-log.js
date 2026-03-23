const fs = require('fs');
const path = require('path');

const WEBUI_ROOT = path.resolve(__dirname, '..');
const DATA_ROOT = path.join(WEBUI_ROOT, 'data', 'users');
const DEFAULT_LOG_PATH = path.resolve(WEBUI_ROOT, '..', 'log.txt');

function usage() {
    console.error('Usage: node tools/import-friends-from-log.js <username> [logPath]');
    process.exit(1);
}

function readVarint(buf, start) {
    let value = 0n;
    let shift = 0n;
    let pos = start;
    while (pos < buf.length) {
        const byte = BigInt(buf[pos]);
        value |= (byte & 0x7fn) << shift;
        pos += 1;
        if ((byte & 0x80n) === 0n) {
            return { value, pos };
        }
        shift += 7n;
    }
    throw new Error(`varint eof at ${start}`);
}

function readLengthDelimited(buf, start) {
    const header = readVarint(buf, start);
    const len = Number(header.value);
    const bodyStart = header.pos;
    if (!Number.isFinite(len) || len < 0) {
        throw new Error(`length out of range at ${start}`);
    }
    // The text dump can lose some in-band CR/LF bytes. When that happens the
    // encoded length is still correct, but the reconstructed buffer is shorter.
    // Clamp to the available buffer so the rest of the record can still be read.
    const bodyEnd = Math.min(bodyStart + len, buf.length);
    return { len, bodyStart, bodyEnd, nextPos: bodyEnd, headerBytes: header.pos - start };
}

function skipField(buf, wireType, start) {
    if (wireType === 0) {
        return readVarint(buf, start).pos;
    }
    if (wireType === 1) {
        return start + 8;
    }
    if (wireType === 2) {
        return readLengthDelimited(buf, start).bodyEnd;
    }
    if (wireType === 5) {
        return start + 4;
    }
    throw new Error(`unsupported wire type ${wireType} at ${start}`);
}

function parseDumpPayload(text) {
    const joined = text.replace(/\r/g, '').split('\n').join('');
    const chunks = [];
    for (let i = 0; i < joined.length;) {
        if (joined[i] === '\\' && joined[i + 1] === 'x' && /^[0-9a-fA-F]{2}$/.test(joined.slice(i + 2, i + 4))) {
            chunks.push(Buffer.from([parseInt(joined.slice(i + 2, i + 4), 16)]));
            i += 4;
            continue;
        }
        const codePoint = joined.codePointAt(i);
        const ch = String.fromCodePoint(codePoint);
        chunks.push(Buffer.from(ch, 'utf8'));
        i += ch.length;
    }
    return Buffer.concat(chunks);
}

function parseFriendFrame(rawBlock) {
    const payloadText = rawBlock.replace(/^.*?\r?\n/, '');
    const buf = parseDumpPayload(payloadText);

    let pos = 0;
    const serviceLen = buf[pos++];
    const service = buf.slice(pos, pos + serviceLen).toString('utf8');
    pos += serviceLen;

    if (buf[pos++] !== 0x12) {
        throw new Error('missing method tag');
    }
    const methodLen = readLengthDelimited(buf, pos);
    const method = buf.slice(methodLen.bodyStart, methodLen.bodyEnd).toString('utf8');
    pos = methodLen.bodyEnd;

    let seenServerSeq = false;
    while (pos < buf.length) {
        const savePos = pos;
        const tagInfo = readVarint(buf, pos);
        const tag = Number(tagInfo.value);
        const field = tag >> 3;
        const wireType = tag & 7;

        if (seenServerSeq && (field < 3 || field > 8)) {
            pos = savePos;
            break;
        }

        pos = tagInfo.pos;
        if ([3, 4, 5, 6].includes(field) && wireType === 0) {
            pos = readVarint(buf, pos).pos;
            if (field === 5) seenServerSeq = true;
            continue;
        }
        if ((field === 7 || field === 8) && wireType === 2) {
            const fieldValue = readLengthDelimited(buf, pos);
            // The dump drops the nested map-entry key tag inside metadata, so the
            // rendered payload is one byte shorter than the encoded length.
            pos = fieldValue.bodyEnd - 1;
            continue;
        }

        pos = savePos;
        break;
    }

    if (buf[pos] === 0x12) {
        pos += 1;
    }
    const bodyLen = readLengthDelimited(buf, pos);
    const body = buf.slice(bodyLen.bodyStart, bodyLen.bodyEnd);
    return { service, method, body };
}

function parsePlantPreview(buf) {
    const preview = {
        stealPlantNum: 0,
        dryNum: 0,
        weedNum: 0,
        insectNum: 0,
    };

    let pos = 0;
    while (pos < buf.length) {
        const tagInfo = readVarint(buf, pos);
        const tag = Number(tagInfo.value);
        const field = tag >> 3;
        const wireType = tag & 7;
        pos = tagInfo.pos;

        if (wireType === 0) {
            const valueInfo = readVarint(buf, pos);
            const value = Number(valueInfo.value);
            pos = valueInfo.pos;
            if (field === 6) preview.stealPlantNum = value;
            if (field === 7) preview.dryNum = value;
            if (field === 8) preview.weedNum = value;
            if (field === 9) preview.insectNum = value;
            continue;
        }

        pos = skipField(buf, wireType, pos);
    }

    return preview;
}

function findAvatarField(body, from, to) {
    const limit = Math.min(to, from + 256);
    for (let pos = from; pos < limit; pos += 1) {
        if (body[pos] !== 0x22) continue;
        try {
            const field = readLengthDelimited(body, pos + 1);
            const head = body.slice(field.bodyStart, Math.min(field.bodyEnd, field.bodyStart + 16)).toString('utf8');
            if (/^https?:\/\//.test(head)) {
                return { tagPos: pos, field };
            }
        } catch (_err) {
            // ignore
        }
    }
    return null;
}

function findFirstPostNameField(body, from, to) {
    const avatar = findAvatarField(body, from, to);
    if (avatar) return avatar.tagPos;

    const limit = Math.min(to, from + 256);
    for (let pos = from; pos < limit; pos += 1) {
        if ([0x2a, 0x30, 0x38, 0x42, 0x4a, 0x50, 0x62].includes(body[pos])) {
            return pos;
        }
    }
    return to;
}

function cleanName(nameBytes) {
    let start = 0;
    let end = nameBytes.length;
    while (start < end && nameBytes[start] < 0x20) start += 1;
    while (end > start && nameBytes[end - 1] < 0x20) end -= 1;

    let text = nameBytes.slice(start, end).toString('utf8').trim();
    text = text.replace(/^\uFFFD+/u, '').replace(/\uFFFD+$/u, '').trim();
    if (!text) return '';

    const first = text[0];
    const rest = text.slice(first.length);
    if (first && first.length === 1) {
        const code = first.charCodeAt(0);
        if (code >= 0x20 && code <= 0x7e && rest && Buffer.byteLength(rest, 'utf8') === code) {
            return rest.trim();
        }
    }
    return text;
}

function scoreName(name) {
    const text = String(name || '').trim();
    if (!text) return -1000;
    let score = 0;
    score += Math.min(80, Buffer.byteLength(text, 'utf8'));
    if (!/\uFFFD/u.test(text)) score += 120;
    if (/[\u4e00-\u9fffA-Za-z0-9]/u.test(text)) score += 25;
    if (/^[!-/:-@[-`{-~]/.test(text)) score -= 6;
    score -= (text.match(/\uFFFD/gu) || []).length * 20;
    return score;
}

function pickBetterName(a, b) {
    return scoreName(a) >= scoreName(b) ? a : b;
}

function normalizePreview(raw) {
    const source = raw && typeof raw === 'object' ? raw : {};
    return {
        stealPlantNum: Math.max(0, Number(source.stealPlantNum || 0) || 0),
        dryNum: Math.max(0, Number(source.dryNum || 0) || 0),
        weedNum: Math.max(0, Number(source.weedNum || 0) || 0),
        insectNum: Math.max(0, Number(source.insectNum || 0) || 0),
    };
}

function extractFriendRows(body) {
    const starts = [];
    for (let pos = 0; pos < body.length - 20; pos += 1) {
        if (body[pos] !== 0x08) continue;
        try {
            const gidInfo = readVarint(body, pos + 1);
            let cursor = gidInfo.pos;
            if (body[cursor] !== 0x12) continue;

            const openIdField = readLengthDelimited(body, cursor + 1);
            if (openIdField.bodyEnd > body.length || openIdField.len < 8 || openIdField.len > 64) continue;
            const openId = body.slice(openIdField.bodyStart, openIdField.bodyEnd).toString('utf8');
            if (!/^oL61M/.test(openId)) continue;

            cursor = openIdField.bodyEnd;
            if (body[cursor] !== 0x1a) continue;

            starts.push({
                start: pos,
                gid: Number(gidInfo.value),
                openId,
                nameStart: cursor + 1,
            });
        } catch (_err) {
            // ignore
        }
    }

    const rows = [];
    for (let i = 0; i < starts.length; i += 1) {
        const current = starts[i];
        const nextStart = starts[i + 1] ? starts[i + 1].start : body.length;
        const firstFieldPos = findFirstPostNameField(body, current.nameStart, nextStart);
        const nameBytes = body.slice(current.nameStart, firstFieldPos);
        const row = {
            gid: current.gid,
            openId: current.openId,
            name: cleanName(nameBytes),
            avatarUrl: '',
            remark: '',
            level: null,
            gold: null,
            preview: null,
        };

        let pos = firstFieldPos;
        const avatar = findAvatarField(body, pos, nextStart);
        if (avatar && avatar.tagPos === pos) {
            row.avatarUrl = body.slice(avatar.field.bodyStart, avatar.field.bodyEnd).toString('utf8');
            pos = avatar.field.bodyEnd;
        }

        if (body[pos] === 0x2a) {
            try {
                const remarkField = readLengthDelimited(body, pos + 1);
                row.remark = cleanName(body.slice(remarkField.bodyStart, remarkField.bodyEnd));
                pos = remarkField.bodyEnd;
            } catch (_err) {
                // ignore
            }
        }

        if (body[pos] === 0x30) {
            try {
                const levelInfo = readVarint(body, pos + 1);
                const level = Number(levelInfo.value);
                if (Number.isFinite(level) && level >= 0 && level <= 200) {
                    row.level = level;
                }
                pos = levelInfo.pos;
            } catch (_err) {
                // ignore
            }
        } else {
            try {
                const levelInfo = readVarint(body, pos);
                const level = Number(levelInfo.value);
                if (
                    Number.isFinite(level)
                    && level >= 0
                    && level <= 200
                    && [0x38, 0x42, 0x4a, 0x50, 0x62].includes(body[levelInfo.pos])
                ) {
                    row.level = level;
                    pos = levelInfo.pos;
                }
            } catch (_err) {
                // ignore
            }
        }

        if (body[pos] === 0x38) {
            try {
                const goldInfo = readVarint(body, pos + 1);
                const gold = Number(goldInfo.value);
                if (Number.isFinite(gold) && gold >= 0) {
                    row.gold = gold;
                }
                pos = goldInfo.pos;
            } catch (_err) {
                // ignore
            }
        }

        if (body[pos] === 0x42) {
            try {
                const tagsField = readLengthDelimited(body, pos + 1);
                pos = tagsField.bodyEnd;
            } catch (_err) {
                // ignore
            }
        }

        if (body[pos] === 0x4a) {
            try {
                const plantField = readLengthDelimited(body, pos + 1);
                row.preview = parsePlantPreview(body.slice(plantField.bodyStart, plantField.bodyEnd));
            } catch (_err) {
                // ignore
            }
        }

        rows.push(row);
    }

    return rows;
}

function mergeRows(existing, incoming) {
    const merged = new Map();
    const order = [];

    const upsert = (row) => {
        if (!row || !Number.isInteger(Number(row.gid)) || Number(row.gid) <= 0) return;
        const gid = Number(row.gid);
        const prev = merged.get(gid);
        if (!prev) {
            merged.set(gid, { ...row, gid });
            order.push(gid);
            return;
        }

        const next = { ...prev };
        next.name = pickBetterName(String(row.name || '').trim(), String(prev.name || '').trim());
        if (scoreName(String(row.remark || '').trim()) > scoreName(String(prev.remark || '').trim())) {
            next.remark = String(row.remark || '').trim();
        }
        if (scoreName(String(row.avatarUrl || '').trim()) > scoreName(String(prev.avatarUrl || '').trim())) {
            next.avatarUrl = String(row.avatarUrl || '').trim();
        }
        if (row.openId) next.openId = row.openId;
        if (Number.isFinite(Number(row.level)) && Number(row.level) >= 0) next.level = Number(row.level);
        if (Number.isFinite(Number(row.gold)) && Number(row.gold) >= 0) next.gold = Number(row.gold);
        if (row.preview && typeof row.preview === 'object') next.preview = normalizePreview(row.preview);
        merged.set(gid, next);
    };

    for (const row of existing) upsert(row);
    for (const row of incoming) upsert(row);

    return order.map((gid) => merged.get(gid)).filter(Boolean);
}

function collectLogRows(logText) {
    const blocks = [...logText.matchAll(/\[(OUTGOING|INCOMING)\]\s*\r?\n([\s\S]*?)(?=\[(?:OUTGOING|INCOMING)\]|$)/g)];
    let bestGetAllRows = [];
    let bestGetAllSource = '';
    const pageRows = [];

    for (const [, direction, rawBlock] of blocks) {
        if (direction !== 'INCOMING' || !rawBlock.includes('gamepb.friendpb.FriendService')) continue;

        let frame;
        try {
            frame = parseFriendFrame(rawBlock);
        } catch (_err) {
            continue;
        }
        if (frame.service !== 'gamepb.friendpb.FriendService') continue;

        const rows = extractFriendRows(frame.body);
        if (frame.method === 'GetAll' && rows.length > bestGetAllRows.length) {
            bestGetAllRows = rows;
            bestGetAllSource = 'GetAll';
        }
        if (frame.method === 'GetGameFriends' && rows.length > 0) {
            pageRows.push(...rows);
        }
    }

    const mergedPageRows = mergeRows([], pageRows);
    const mergedRows = mergeRows(bestGetAllRows, mergedPageRows);
    const sourceLabel = bestGetAllSource
        ? `${bestGetAllSource}${mergedPageRows.length ? '+GetGameFriends' : ''}`
        : 'GetGameFriends';

    return {
        sourceLabel,
        rows: mergedRows,
        getAllCount: bestGetAllRows.length,
        getGameFriendsCount: mergedPageRows.length,
    };
}

function mergeWithExistingSnapshot(parsedRows, existingSnapshot) {
    const existingMap = new Map();
    for (const raw of Array.isArray(existingSnapshot && existingSnapshot.friends) ? existingSnapshot.friends : []) {
        if (!raw || typeof raw !== 'object') continue;
        const gid = Number(raw.gid);
        if (!Number.isInteger(gid) || gid <= 0) continue;
        existingMap.set(gid, raw);
    }

    return parsedRows.map((row) => {
        const previous = existingMap.get(Number(row.gid)) || {};
        const parsedName = String(row.name || '').trim();
        const previousName = String(previous.name || previous.nick || '').trim();
        const previousRemark = String(previous.remark || '').trim();
        const name = pickBetterName(parsedName, pickBetterName(previousRemark, previousName));

        return {
            gid: Number(row.gid),
            name,
            nick: parsedName || String(previous.nick || previous.name || '').trim() || name,
            remark: String(row.remark || previous.remark || '').trim(),
            level: Number.isFinite(Number(row.level)) && Number(row.level) >= 0
                ? Number(row.level)
                : Math.max(0, Number(previous.level || 0) || 0),
            gold: Number.isFinite(Number(row.gold)) && Number(row.gold) >= 0
                ? Number(row.gold)
                : Math.max(0, Number(previous.gold || 0) || 0),
            preview: row.preview
                ? normalizePreview(row.preview)
                : normalizePreview(previous.preview),
        };
    });
}

function main() {
    const username = String(process.argv[2] || '').trim().toLowerCase();
    const logPathInput = String(process.argv[3] || DEFAULT_LOG_PATH).trim();
    if (!username) usage();

    const logPath = path.isAbsolute(logPathInput)
        ? logPathInput
        : path.resolve(process.cwd(), logPathInput);
    if (!fs.existsSync(logPath)) {
        throw new Error(`log file not found: ${logPath}`);
    }

    const userDir = path.join(DATA_ROOT, username);
    const friendsPath = path.join(userDir, 'friends.json');
    const logText = fs.readFileSync(logPath, 'utf8');
    const capture = collectLogRows(logText);
    if (!capture.rows.length) {
        throw new Error('no friend rows extracted from log');
    }

    let previousSnapshot = { friends: [] };
    if (fs.existsSync(friendsPath)) {
        previousSnapshot = JSON.parse(fs.readFileSync(friendsPath, 'utf8')) || { friends: [] };
    }

    const mergedFriends = mergeWithExistingSnapshot(capture.rows, previousSnapshot);
    const stat = fs.statSync(logPath);
    const payload = {
        version: 1,
        source: `log-import:${capture.sourceLabel}`,
        updated_at: stat.mtime.toISOString(),
        total: mergedFriends.length,
        friends: mergedFriends,
    };

    fs.mkdirSync(userDir, { recursive: true });
    const tmpPath = `${friendsPath}.tmp`;
    fs.writeFileSync(tmpPath, `${JSON.stringify(payload)}\n`, 'utf8');
    fs.renameSync(tmpPath, friendsPath);

    console.log(`Imported ${mergedFriends.length} friends for ${username}`);
    console.log(`log: ${logPath}`);
    console.log(`GetAll rows: ${capture.getAllCount}`);
    console.log(`GetGameFriends rows: ${capture.getGameFriendsCount}`);
    console.log(`snapshot: ${friendsPath}`);
}

if (require.main === module) {
    try {
        main();
    } catch (err) {
        console.error(err && err.message ? err.message : String(err));
        process.exit(1);
    }
}

module.exports = {
    collectLogRows,
    extractFriendRows,
    parseDumpPayload,
    parseFriendFrame,
    readLengthDelimited,
    readVarint,
};
