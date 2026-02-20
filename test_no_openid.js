#!/usr/bin/env node

/**
 * 测试不带 openID 参数的连接
 */

const WebSocket = require("ws");

if (process.argv.length < 3) {
  console.error('用法: node test_no_openid.js <code>');
  process.exit(1);
}

const code = process.argv[2];
const platform = 'wx';
const os = 'iOS';
const ver = '1.6.0.11_20251224';

// 不包含 openID 参数
const url = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}`;

console.log('========================================');
console.log('测试不带 openID 参数的连接');
console.log('========================================');
console.log('URL:', url);
console.log('');

const ws = new WebSocket(url, {
  headers: {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Origin": "weapp://wechat-game-runtime",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh-Hans;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
  },
});

ws.on("open", () => {
  console.log('✅ WebSocket 连接成功!');
  console.log('');
  console.log('看起来移除空的 openID 参数就是解决方案！');
  ws.close();
  process.exit(0);
});

ws.on("unexpected-response", (req, res) => {
  console.log(`❌ HTTP ${res.statusCode}`);
  let body = '';
  res.on('data', chunk => {
    body += chunk.toString();
  });
  res.on('end', () => {
    if (body) {
      console.log('响应:', body);
    }
    console.log('');
    process.exit(1);
  });
});

ws.on("error", (err) => {
  console.log('❌ 错误:', err.message);
  console.log('');
  process.exit(1);
});

setTimeout(() => {
  console.log('❌ 超时');
  ws.close();
  process.exit(1);
}, 10000);
