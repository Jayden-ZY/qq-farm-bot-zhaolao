#!/usr/bin/env node

/**
 * 测试不同的 openID 值
 */

const WebSocket = require("ws");

if (process.argv.length < 3) {
  console.error('用法: node test_openid_values.js <code>');
  process.exit(1);
}

const code = process.argv[2];
const platform = 'wx';
const os = 'iOS';
const ver = '1.6.0.11_20251224';

const testValues = [
  { name: 'undefined', value: 'undefined' },
  { name: 'null', value: 'null' },
  { name: '空字符串', value: '' },
  { name: '0', value: '0' },
  { name: 'test', value: 'test' },
];

let currentTest = 0;

function runNextTest() {
  if (currentTest >= testValues.length) {
    console.log('\n❌ 所有测试值都失败了');
    console.log('\n你需要提供真实的 openID 值。');
    console.log('请从抓包工具或微信开发者工具的 Network 面板中获取完整的 WebSocket URL。');
    process.exit(1);
  }

  const test = testValues[currentTest];
  const url = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}&openID=${test.value}`;

  console.log(`\n[测试 ${currentTest + 1}/${testValues.length}] openID = "${test.name}"`);
  console.log('URL:', url);

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

  const timeout = setTimeout(() => {
    console.log('❌ 超时');
    ws.close();
    currentTest++;
    setTimeout(runNextTest, 100);
  }, 5000);

  ws.on("open", () => {
    clearTimeout(timeout);
    console.log('✅ 连接成功！');
    console.log(`\n🎉 openID = "${test.name}" 是有效的！`);
    ws.close();
    process.exit(0);
  });

  ws.on("unexpected-response", (req, res) => {
    clearTimeout(timeout);
    let body = '';
    res.on('data', chunk => {
      body += chunk.toString();
    });
    res.on('end', () => {
      console.log(`❌ HTTP ${res.statusCode}: ${body}`);
      currentTest++;
      setTimeout(runNextTest, 100);
    });
  });

  ws.on("error", (err) => {
    clearTimeout(timeout);
    console.log('❌ 错误:', err.message);
    currentTest++;
    setTimeout(runNextTest, 100);
  });
}

console.log('========================================');
console.log('测试不同的 openID 值');
console.log('========================================');

runNextTest();
