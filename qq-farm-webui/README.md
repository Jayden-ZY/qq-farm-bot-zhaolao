# qq-farm-webui

`qq-farm-bot` 的本地 WEB 控制台。

## 功能

- 启动 / 停止 / 重启机器人
- 配置 `platform / code / 间隔 / allowMulti / extraArgs`
- 实时查看日志（支持标签过滤和关键词搜索）
- 展示运行指标（等级、金币、经验、收获、偷菜、任务、告警）
- 可视化编辑 `qq-farm-bot/share.txt`

## 前置条件

- Node.js `18+`（建议 `20+`）
- 同级目录存在 `qq-farm-bot`
- `qq-farm-bot` 已完成 `npm install`

目录关系：

```text
农场/
├── qq-farm-bot/
└── qq-farm-webui/
```

## 启动

```bash
cd qq-farm-webui
npm install
npm start
```

默认地址：

```text
http://localhost:3737
```

## 使用建议

1. 先在网页里保存 `code` 和间隔配置。
2. 点击“保存并启动”。
3. 在“实时日志”观察状态；如有异常，先看“错误”过滤。

## 说明

- WEBUI 通过 `qq-farm-bot/start.sh` 和 `qq-farm-bot/kill.sh` 控制进程。
- 日志来自 `qq-farm-bot/logs/farm.log`。
- 配置保存于 `qq-farm-webui/data/config.json`。
