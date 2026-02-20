# 如何获取微信小程序 CODE

## 重要说明

⚠️ **微信小程序的 code 是临时凭证，具有以下特点：**
- **有效期短**：通常只有 5 分钟
- **一次性使用**：每个 code 只能使用一次，使用后立即失效
- **每次都要重新获取**：每次运行程序前都需要获取新的 code

## 方法一：使用微信开发者工具（推荐）

### 步骤 1: 安装微信开发者工具
1. 下载并安装[微信开发者工具](https://developers.weixin.qq.com/miniprogram/dev/devtools/download.html)
2. 使用微信扫码登录

### 步骤 2: 打开 QQ 经典农场小程序
1. 在微信开发者工具中，选择"小程序"
2. 导入项目或直接打开 QQ 经典农场小程序的调试版本
3. 也可以使用真机调试功能

### 步骤 3: 获取 code
在控制台（Console）中输入以下代码：

```javascript
wx.login({
  success: function(res) {
    console.log('你的 code 是:', res.code);
  }
});
```

或者更简洁的写法：

```javascript
wx.login({success: res => console.log(res.code)})
```

控制台会输出类似这样的 code：
```
0d12rX0w3bsDu63Kg92w36GGbj22rX0f
```

### 步骤 4: 立即使用 code
**获取 code 后，必须立即使用！**

```bash
# 复制你刚获取的 code，替换下面的 YOUR_CODE
node client.js --code YOUR_CODE --wx
```

## 方法二：使用抓包工具

### 使用 Charles 或 Fiddler 抓包

1. 在手机上配置代理，指向你的电脑
2. 安装并信任 SSL 证书
3. 在微信中打开 QQ 经典农场小程序
4. 在抓包工具中查找连接到 `gate-obt.nqf.qq.com` 的 WebSocket 请求
5. 在 URL 参数中找到 `code=xxx` 部分
6. 复制 code 值

**注意：** 抓包获取的 code 可能已经被小程序使用过了，所以可能无法再次使用。建议使用方法一。

## 常见问题

### Q: 为什么总是报错 400 Bad Request？

A: 最常见的原因是：
1. **code 已过期**：从获取到使用超过了 5 分钟
2. **code 已被使用**：该 code 已经在其他地方使用过了
3. **code 格式错误**：复制时漏掉了字符或多了空格

**解决方法：** 重新获取一个新的 code，并立即使用

### Q: 每次运行都要获取新的 code 吗？

A: 是的！这是微信小程序的安全机制。每次运行程序前都需要：
1. 获取新的 code
2. 立即运行程序

### Q: 可以自动化这个过程吗？

A: 理论上可以，但需要：
1. 模拟微信客户端的登录流程
2. 这违反了微信的使用条款
3. 不建议这样做

### Q: 抓包获取的 code 为什么不能用？

A: 当你在抓包工具中看到 code 时，微信小程序可能已经使用过这个 code 了。因为：
- 小程序在启动时会自动调用 `wx.login()` 获取 code
- 然后立即使用这个 code 连接服务器
- 所以你抓到的 code 通常已经被使用过了

## 测试连接

使用我们提供的测试脚本来验证 code 是否有效：

```bash
node test_connection.js YOUR_CODE
```

如果连接成功，你会看到：
```
✅ WebSocket 连接成功!
```

如果失败，脚本会告诉你可能的原因和解决方法。

## 完整使用流程

```bash
# 1. 在微信开发者工具中获取 code
wx.login({success: res => console.log(res.code)})

# 2. 复制输出的 code（例如：0d12rX0w3bsDu63Kg92w36GGbj22rX0f）

# 3. 立即测试连接（可选）
node test_connection.js 0d12rX0w3bsDu63Kg92w36GGbj22rX0f

# 4. 运行完整程序
node client.js --code 0d12rX0w3bsDu63Kg92w36GGbj22rX0f --wx

# 5. 如果报错 400，重复步骤 1-4
```

## 自动化建议

如果你想长期运行这个程序，建议：

1. **使用微信开发者工具的远程调试功能**
   - 可以在手机上调试，在电脑上获取 code
   - 更方便快捷

2. **创建一个简单的获取脚本**
   ```bash
   # 创建一个 shell 脚本 run.sh
   #!/bin/bash
   echo "请在微信开发者工具中运行: wx.login({success: res => console.log(res.code)})"
   echo "然后输入你获取到的 code:"
   read code
   node client.js --code $code --wx
   ```

3. **使用持久化的 session_key**（高级）
   - 需要自己搭建服务器
   - 将 code 换取 session_key
   - session_key 可以持续使用更长时间
   - 但实现比较复杂

## 技术原理

微信小程序的登录流程：
1. 小程序调用 `wx.login()` 获取临时凭证 code
2. 将 code 发送到服务器
3. 服务器用 code 向微信服务器换取 openid 和 session_key
4. 每个 code 只能换取一次，换取后立即失效

这就是为什么 code 只能使用一次的原因。
