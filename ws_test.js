
const WebSocket = require("ws");

const url = process.argv[2];
console.log("URL =", url);

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

ws.on("open", () => console.log("OPEN"));
ws.on("unexpected-response", (req, res) =>
  console.log("UNEXPECTED", res.statusCode)
);
ws.on("error", (e) => console.error("ERR", e.message));
ws.on("close", (c) => console.log("CLOSE", c));
