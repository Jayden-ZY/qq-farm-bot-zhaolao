#!/usr/bin/env node

/**
 * WebSocket 连接测试工具
 * 用法: node test_connection.js <你的微信code>
 */

const WebSocket = require("ws");

if (process.argv.length < 3) {
  console.error('用法: node test_connection.js <code>');
  console.error('');
  console.error('说明:');
  console.error('  code 必须是刚从微信小程序获取的临时凭证');
  console.error('  code 有效期很短(约5分钟)，且只能使用一次');
  console.error('');
  console.error('如何获取 code:');
  console.error('  1. 打开微信开发者工具');
  console.error('  2. 在控制台输入: wx.login({success: res => console.log(res.code)})');
  console.error('  3. 复制输出的 code');
  process.exit(1);
}

const code = process.argv[2];
const platform = 'wx';
const os = 'iOS';
const ver = '1.6.0.11_20251224';

const url = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}&openID=`;

console.log('========================================');
console.log('WebSocket 连接测试');
console.log('========================================');
console.log('URL:', url);
console.log('');
console.log('Headers:');

const headers = {
  "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
  "Origin": "weapp://wechat-game-runtime",
  "Accept": "*/*",
  "Accept-Language": "zh-CN,zh-Hans;q=0.9",
  "Accept-Encoding": "gzip, deflate",
  "Cache-Control": "no-cache",
  "Pragma": "no-cache",
};

Object.entries(headers).forEach(([key, value]) => {
  console.log(`  ${key}: ${value}`);
});

console.log('');
console.log('正在连接...');
console.log('');

const ws = new WebSocket(url, { headers });

ws.on("open", () => {
  console.log('✅ WebSocket 连接成功!');
  console.log('');
  console.log('如果看到这条消息，说明你的配置是正确的。');
  console.log('现在可以使用 node client.js --code <你的code> --wx 来运行完整程序');
  ws.close();
  process.exit(0);
});

ws.on("unexpected-response", (req, res) => {
  console.log(`❌ 连接失败: HTTP ${res.statusCode}`);
  console.log('');

  if (res.statusCode === 400) {
    console.log('可能的原因:');
    console.log('  1. code 已过期（code 有效期约5分钟）');
    console.log('  2. code 已被使用过（每个 code 只能使用一次）');
    console.log('  3. code 格式不正确');
    console.log('');
    console.log('解决方法:');
    console.log('  - 重新从微信小程序获取新的 code');
    console.log('  - 确保获取 code 后立即使用');
  } else if (res.statusCode === 403) {
    console.log('可能的原因:');
    console.log('  - 服务器拒绝连接');
    console.log('  - IP 被限制');
  } else {
    console.log('未知错误，状态码:', res.statusCode);
  }

  console.log('');
  process.exit(1);
});

ws.on("error", (err) => {
  console.log('❌ WebSocket 错误:', err.message);
  console.log('');

  if (err.message.includes('ENOTFOUND') || err.message.includes('ECONNREFUSED')) {
    console.log('网络连接问题:');
    console.log('  - 检查网络连接');
    console.log('  - 检查防火墙设置');
  }

  console.log('');
  process.exit(1);
});

ws.on("close", (code) => {
  if (code !== 1000) {
    console.log(`连接关闭 (code=${code})`);
  }
});

// 10秒超时
setTimeout(() => {
  console.log('❌ 连接超时');
  console.log('');
  console.log('可能的原因:');
  console.log('  - 网络连接问题');
  console.log('  - 服务器无响应');
  ws.close();
  process.exit(1);
}, 10000);
