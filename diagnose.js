#!/usr/bin/env node

/**
 * WebSocket 连接诊断工具
 * 尝试不同的配置组合来找出问题
 */

const WebSocket = require("ws");

if (process.argv.length < 3) {
  console.error('用法: node diagnose.js <code>');
  process.exit(1);
}

const code = process.argv[2];
const platform = 'wx';
const os = 'iOS';
const ver = '1.6.0.11_20251224';

const baseUrl = `wss://gate-obt.nqf.qq.com/prod/ws?platform=${platform}&os=${os}&ver=${ver}&code=${code}&openID=`;

// 测试配置列表
const testConfigs = [
  {
    name: "配置1: 完整 headers + perMessageDeflate",
    url: baseUrl,
    options: {
      headers: {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Origin": "weapp://wechat-game-runtime",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
      },
      perMessageDeflate: true,
    }
  },
  {
    name: "配置2: 完整 headers + 禁用 perMessageDeflate",
    url: baseUrl,
    options: {
      headers: {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Origin": "weapp://wechat-game-runtime",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
      },
      perMessageDeflate: false,
    }
  },
  {
    name: "配置3: 只有基本 headers",
    url: baseUrl,
    options: {
      headers: {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Origin": "weapp://wechat-game-runtime",
      },
    }
  },
  {
    name: "配置4: 完整 headers + Sec-Fetch headers",
    url: baseUrl,
    options: {
      headers: {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Origin": "weapp://wechat-game-runtime",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "websocket",
        "Sec-Fetch-Dest": "websocket",
      },
    }
  },
];

let currentTest = 0;
let allFailed = true;

function runNextTest() {
  if (currentTest >= testConfigs.length) {
    if (allFailed) {
      console.log('\n========================================');
      console.log('❌ 所有配置都失败了');
      console.log('========================================\n');
      console.log('可能的原因:');
      console.log('  1. code 确实已过期或被使用过');
      console.log('  2. 服务器可能需要其他验证机制');
      console.log('  3. 可能需要先通过其他 API 进行预认证\n');
      console.log('建议:');
      console.log('  - 重新获取一个全新的 code');
      console.log('  - 确保在获取 code 后的 1 分钟内使用');
      console.log('  - 检查微信小程序是否有更新的认证流程\n');
    }
    process.exit(allFailed ? 1 : 0);
  }

  const config = testConfigs[currentTest];
  console.log(`\n[测试 ${currentTest + 1}/${testConfigs.length}] ${config.name}`);
  console.log('----------------------------------------');

  const ws = new WebSocket(config.url, config.options);

  const timeout = setTimeout(() => {
    console.log('❌ 超时');
    ws.close();
    currentTest++;
    setTimeout(runNextTest, 100);
  }, 5000);

  ws.on("open", () => {
    clearTimeout(timeout);
    console.log('✅ 连接成功！');
    console.log('\n========================================');
    console.log('🎉 找到工作的配置！');
    console.log('========================================\n');
    console.log('配置详情:');
    console.log(JSON.stringify(config.options, null, 2));
    console.log('\n');
    allFailed = false;
    ws.close();
    process.exit(0);
  });

  ws.on("unexpected-response", (req, res) => {
    clearTimeout(timeout);
    console.log(`❌ HTTP ${res.statusCode}`);

    // 读取响应体
    let body = '';
    res.on('data', chunk => {
      body += chunk.toString();
    });
    res.on('end', () => {
      if (body) {
        console.log('响应体:', body);
      }
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
console.log('WebSocket 连接诊断');
console.log('========================================');
console.log('URL:', baseUrl);
console.log(`\n开始测试 ${testConfigs.length} 种不同的配置...\n`);

runNextTest();
