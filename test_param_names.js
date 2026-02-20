#!/usr/bin/env node

/**
 * 测试不同的参数名
 */

const WebSocket = require("ws");

if (process.argv.length < 3) {
  console.error('用法: node test_param_names.js <code>');
  process.exit(1);
}

const code = process.argv[2];
const platform = 'wx';
const os = 'iOS';
const ver = '1.6.0.11_20251224';

const testConfigs = [
  { name: 'openID（驼峰）', param: 'openID', value: '' },
  { name: 'openid（全小写）', param: 'openid', value: '' },
  { name: 'openlD（错误拼写）', param: 'openlD', value: '' },
  { name: 'openId（首字母小写）', param: 'openId', value: '' },
  { name: '不带 openID 参数', param: null, value: '' },
];

let currentTest = 0;

function runNextTest() {
  if (currentTest >= testConfigs.length) {
    console.log('\n========================================');
    console.log('❌ 所有测试都失败了');
    console.log('========================================\n');
    console.log('结论：服务器确实需要 openID 参数，且必须有值！\n');
    console.log('你的抓包显示 openID 是空的，这说明：');
    console.log('  1. 这个请求可能失败了（返回 400）');
    console.log('  2. 或者抓包工具没有显示完整的值\n');
    console.log('建议：');
    console.log('  - 在抓包工具中查看响应状态码');
    console.log('  - 只查看状态码为 101 的成功请求');
    console.log('  - 或者使用断点拦截，获取完整的 URL\n');
    process.exit(1);
  }

  const config = testConfigs[currentTest];

  let url;
  if (config.param === null) {
    url = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}`;
  } else {
    url = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}&${config.param}=${config.value}`;
  }

  console.log(`\n[测试 ${currentTest + 1}/${testConfigs.length}] ${config.name}`);
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
    console.log(`\n🎉 找到正确的参数名：${config.param || '不需要 openID'}`);
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
console.log('测试不同的参数名');
console.log('========================================');

runNextTest();
