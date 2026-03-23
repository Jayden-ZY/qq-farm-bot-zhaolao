const https = require('https');

let nodemailerLoadError = null;
const transporterCache = new Map();

function getNodemailer() {
    if (nodemailerLoadError) throw nodemailerLoadError;
    try {
        // eslint-disable-next-line global-require, import/no-extraneous-dependencies
        return require('nodemailer');
    } catch (_err) {
        nodemailerLoadError = new Error('未安装 nodemailer，请在 qq-farm-webui 目录执行 npm install');
        throw nodemailerLoadError;
    }
}

function normalizeSmtpConfig(raw = {}) {
    const hostValue = raw.smtpHost !== undefined ? raw.smtpHost : raw.host;
    const portValue = raw.smtpPort !== undefined ? raw.smtpPort : raw.port;
    const userValue = raw.smtpUser !== undefined ? raw.smtpUser : raw.user;
    const passValue = raw.smtpPass !== undefined ? raw.smtpPass : raw.pass;
    const fromNameValue = raw.smtpFromName !== undefined ? raw.smtpFromName : raw.fromName;
    return {
        host: String(hostValue || '').trim(),
        port: Number(portValue || 0),
        user: String(userValue || '').trim(),
        pass: String(passValue || '').trim(),
        fromName: String(fromNameValue || '').trim() || 'QQ Farm Bot',
    };
}

function getTransporter(smtpConfig) {
    const smtp = normalizeSmtpConfig(smtpConfig);
    if (!smtp.host || !smtp.port || !smtp.user || !smtp.pass) {
        throw new Error('SMTP 未配置完整，请填写主机、端口、账号和授权码');
    }

    const cacheKey = JSON.stringify([smtp.host, smtp.port, smtp.user, smtp.pass]);
    if (transporterCache.has(cacheKey)) {
        return transporterCache.get(cacheKey);
    }

    const nodemailer = getNodemailer();
    const transporter = nodemailer.createTransport({
        host: smtp.host,
        port: smtp.port,
        secure: smtp.port === 465,
        auth: {
            user: smtp.user,
            pass: smtp.pass,
        },
        tls: { rejectUnauthorized: false },
        connectionTimeout: 15000,
        greetingTimeout: 15000,
        socketTimeout: 15000,
    });
    transporterCache.set(cacheKey, transporter);
    return transporter;
}

async function sendMail({ to, subject, html, smtpConfig }) {
    const target = String(to || '').trim();
    if (!target) throw new Error('未配置接收邮箱');

    const smtp = normalizeSmtpConfig(smtpConfig);
    const transporter = getTransporter(smtp);
    await transporter.sendMail({
        from: `"${smtp.fromName}" <${smtp.user}>`,
        to: target,
        subject: String(subject || '').trim() || 'QQ Farm Bot 通知',
        html: String(html || '').trim() || '<p>空通知</p>',
    });
}

function sendServerChan({ type = 'sc3', key, title, desp }) {
    const sendKey = String(key || '').trim();
    if (!sendKey) {
        return Promise.reject(new Error('未配置 Server酱 SendKey'));
    }

    const postData = new URLSearchParams({
        title: String(title || '').trim() || 'QQ Farm Bot 通知',
        desp: String(desp || '').trim() || '空通知',
    }).toString();

    let hostname = 'push.ft07.com';
    let pathname = `/send/${sendKey}.send`;
    if (String(type || '').trim().toLowerCase() === 'turbo') {
        hostname = 'sctapi.ftqq.com';
        pathname = `/${sendKey}.send`;
    } else {
        const uidMatch = sendKey.match(/^sctp(\d+)t/i);
        if (uidMatch) hostname = `${uidMatch[1]}.push.ft07.com`;
    }

    return new Promise((resolve, reject) => {
        const req = https.request({
            hostname,
            path: pathname,
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Content-Length': Buffer.byteLength(postData),
            },
        }, (res) => {
            let body = '';
            res.on('data', (chunk) => { body += chunk; });
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(body);
                    if (parsed && Number(parsed.code) === 0) {
                        resolve(parsed);
                        return;
                    }
                    reject(new Error(parsed && parsed.message ? parsed.message : `Server酱返回异常: ${body}`));
                } catch (_err) {
                    reject(new Error(`Server酱响应解析失败: ${body}`));
                }
            });
        });
        req.on('error', reject);
        req.write(postData);
        req.end();
    });
}

async function pushNotification(payload, settings) {
    const title = String((payload && payload.title) || '').trim() || 'QQ Farm Bot 通知';
    const markdown = String((payload && payload.markdown) || '').trim() || title;
    const html = String((payload && payload.html) || '').trim() || `<pre>${markdown}</pre>`;
    const channels = payload && payload.channels && typeof payload.channels === 'object' ? payload.channels : {};
    const global = settings && typeof settings === 'object' ? settings : {};

    const tasks = [];
    const results = [];

    if (channels.email && global.emailEnabled && global.mailTo) {
        tasks.push(
            sendMail({
                to: global.mailTo,
                subject: title,
                html,
                smtpConfig: global,
            })
                .then(() => results.push({ channel: 'email', ok: true }))
                .catch((err) => results.push({ channel: 'email', ok: false, error: err.message }))
        );
    }

    if (channels.serverChan && global.serverChanEnabled && global.serverChanKey) {
        tasks.push(
            sendServerChan({
                type: global.serverChanType,
                key: global.serverChanKey,
                title,
                desp: markdown,
            })
                .then(() => results.push({ channel: 'serverChan', ok: true }))
                .catch((err) => results.push({ channel: 'serverChan', ok: false, error: err.message }))
        );
    }

    if (tasks.length <= 0) {
        return {
            ok: false,
            skipped: true,
            reason: '没有启用的通知渠道',
            results,
        };
    }

    await Promise.all(tasks);
    return {
        ok: results.some((item) => item.ok),
        skipped: false,
        results,
    };
}

module.exports = {
    pushNotification,
};
