# EverProxy

> 将动态/短效代理 API 无缝转化为本地长效静态代理服务。纯 Python 标准库实现，零第三方依赖，支持备胎池热替换与心跳保活。

![Python Version](https://img.shields.io/badge/python-3.6%2B-blue)
![Dependencies](https://img.shields.io/badge/dependencies-0%20(Standard%20Library)-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)
![Protocol](https://img.shields.io/badge/protocol-HTTP%20%7C%20HTTPS%20%7C%20SOCKS5-orange)

---

## 📖 项目简介

在进行自动化测试、爬虫开发或软件连网时，我们往往会购买短效动态代理 API。但它们存在以下痛点：
* 有效期短，频繁失效导致频繁掉线；
* 格式混乱（TXT、JSON 各异，字段结构各异）；
* 每次切换 IP 需要在应用端重新修改配置。

**EverProxy** 可以在本地建立一个**长效稳固的代理服务端（如 `127.0.0.1:1217`）**：
* 你的其他软件/脚本只需**连接这一个固定地址**；
* 上游短效 IP 的**获取、测速、心跳保活、地理位置查询、失效热替换、后台备胎预取**，全部交给 EverProxy 自动后台处理；
* **整个替换过程对业务完全无感、网络不中断**。

---

## ✨ 核心特性

- 📦 **零外部依赖**：仅使用 Python 标准库（`ssl` / `socketserver` / `threading` / `concurrent.futures` 等），无需 `pip install` 任何模块。
- 🔄 **后台备胎预取（Standby Pool）**：在当前 IP 即将过期之前，自动提前验证并预取新 IP；当触发更换时**毫秒级无缝切换**。
- 💓 **智能心跳与错误断路**：自动通过测试 URL 探测当前 IP 活性，心跳失效或连续 3 次请求错误时立即自动切换。
- 🌐 **全协议自动识别**：
  - **上游识别**：自动探测支持 HTTP、HTTPS、SOCKS5；
  - **本地服务端**：同端口同时兼容并识别 HTTP(S) 与 SOCKS5 客户端流量。
- 🧩 **全格式 API 兼容**：自动识别 API 返回值格式（TXT 行分割 / JSON 自定义路径解析），支持复杂的嵌套 JSON 解析。
- ⚡ **多线程并发测速**：可配置并发多线程测速，自动挑选当前响应最快的候选代理。
- 📍 **地理位置显示**：自动聚合显示当前选中 IP 的归属地位置。
- 🌍 **双语界面支持**：内置完善的简中 (CN) 与英文 (EN) 控制台日志文案。

---

## 🏗️ 工作原理与流程

```
+-------------------------------------------------------------------------+
|                              EverProxy                                  |
|                                                                         |
|  [ 上游动态代理 API ] ---> ( TXT / JSON 解析 )                          |
|                                |                                        |
|                         ( 多线程响应测速 )                              |
|                                |                                        |
|                     [ 预取备胎池 (Standby Pool) ]                       |
|                                |                                        |
|  +-----------------------------+-----------------------------+          |
|  |                             |                             |          |
|  v                             v                             v          |
| [ 当前活动代理 ] --(心跳探测/连续报错触发)--> [ 瞬间置换 ]    |          |
|        |                                                     |          |
+--------|-----------------------------------------------------|----------+
                                                         ^
                                                         |
[ 本地长效代理地址: 127.0.0.1:1217 ] <---(HTTP / HTTPS / SOCKS5)-+
                                                         |
                                              应用客户端 (浏览器 / 爬虫脚本 / 自动化软件)
```

---

## 🚀 快速上手

### 1. 克隆项目

```bash
git clone https://github.com/YourUsername/EverProxy.git
cd EverProxy
```

### 2. 配置参数

编辑目录下的 `config.ini` 文件，填入你的短效代理 API 链接：

```ini
[proxy]
# 将这里修改为你自己的提取 API URL
api_url = https://your-proxy-provider.com/get?num=5&format=txt

# 监听端口号（例如设置后即可在应用程序中使用 127.0.0.1:1217）
listen_port = 1217
```

### 3. 运行服务

由于本项目无任何依赖，直接执行脚本即可：

```bash
python3 proxy.py
# 或显式指定配置文件路径
python3 proxy.py /path/to/config.ini
```

控制台出现以下输出即代表启动成功：

```text
加载配置文件: config.ini
API 返回 5 个代理候选
检测到 API 返回格式: TXT
代理 114.12.34.56:8080 [socks5] 可用, 延迟 0.35s
已切换到新代理: 114.12.34.56:8080 [socks5]
当前代理地理位置: 广东省深圳市 某某网络
代理轮换服务已启动 | 本地监听 127.0.0.1:1217 | 输出协议: socks5
请在其他软件中填写代理地址 127.0.0.1:1217 即可使用
```

---

## ⚙️ 参数配置说明 (`config.ini`)

| 参数项 | 默认值 | 可选值/说明 |
| --- | --- | --- |
| `api_url` | *(空)* | **[必填]** 从服务商处获取代理 IP 列表的 API URL |
| `input_protocol` | `auto` | `auto`, `http`, `https`, `socks5`。为 `auto` 时优先测试 `socks5` |
| `output_protocol` | `same` | `same`, `http`, `https`, `socks5`。`same` 表示本地服务端协议与当前上游协议一致 |
| `listen_port` | `1217` | 本地服务监听端口 |
| `force_change_interval` | `60` | 强制更换为新 IP 的间隔时间（秒） |
| `heartbeat_interval` | `10` | 代理心跳检测间隔（秒），失败立刻热切换 |
| `standby_pool_size` | `2` | 后台预留备用可用代理的数量，填 `0` 则关闭预取 |
| `multithread_test` | `false` | `true`/`false`，开启时多线程对候选代理进行测速，选用最快的 IP |
| `show_location` | `true` | `true`/`false`，是否显示查询到的地理位置信息 |
| `language` | `cn` | `cn` (中文) / `en` (English) |
| `max_retries` | `10` | 获取代理的最大重试次数（指数退避），`0` 为无限重试 |
| `timeout` | `3` | 测试和查询连接的超时时间（秒） |
| `txt_separator` | *(空)* | API 返回 TXT 格式时 IP 之间的分隔符；留空为默认按换行分割 |
| `json_ip_paths` | *(空)* | API 返回 JSON 时的 IP 解析路径（支持用分号写多组匹配） |
| `json_port_path` | *(空)* | 若 IP 与端口分离时，指定端口字段路径 |

---

## 🔍 JSON 格式 API 配置范例

如果你的服务商 API 返回的是 JSON 格式内容，例如：

```json
{
  "code": 0,
  "data": [
    { "proxy_ip": "103.23.45.67", "proxy_port": 1080 },
    { "proxy_ip": "103.23.45.68", "proxy_port": 1080 }
  ]
}
```

你只需要在 `config.ini` 里进行如下路径配置（支持通配符 `[*]` 提取列表）：

```ini
json_ip_paths = data[*].proxy_ip
json_port_path = data[*].proxy_port
```

*如果 API 已经把 IP 和端口组合在了一个字段中（如 `"ip": "103.23.45.67:1080"`），则直接设定 `json_ip_paths = data[*].ip`，并将 `json_port_path` 留空即可。*

---

## 🛠️ 常见问题 (FAQ)

### 1. 为什么无需在客户端反复修改代理地址？

程序在本地通过 `socketserver` 开启了一个静态不变的端口。应用的请求会发往本地地址，随后由该工具全权负责在上游切换不同动态 IP 并重新封装数据包转发。

### 2. 备胎池（Standby Pool）机制是如何避免断网的？

传统代理在过期或心跳挂掉后，程序才发起网络请求获取新 IP，这一过程通常需要 1~3 秒，容易导致应用报错。

开启备胎池（`standby_pool_size > 0`）后，工作线程会在运行期提前获取并验证有效 IP 存放在缓冲池；当旧 IP 到期、失效或触发失败封锁时，从缓冲池中取出备用 IP **瞬间替换**。

### 3. 如何集成到 Python Request 或 Selenium / Playwright？

**Python Requests 示例**：

```python
import requests

proxies = {
    "http": "http://127.0.0.1:1217",
    "https": "http://127.0.0.1:1217",
    # 如果代理服务自动识别为 socks5，也可直接传入
    # "http": "socks5://127.0.0.1:1217"
}

response = requests.get("https://httpbin.org/ip", proxies=proxies)
print(response.json())
```

**Selenium / Playwright 示例**：

在浏览器初始化配置参数中直接传入代理主机：`--proxy-server=127.0.0.1:1217` 即可。

---

## 🤝 参与贡献

欢迎通过 Issue 或 Pull Request 为本项目提供建议与特性改进！由于本项目的核心设计宗旨之一是 **"纯 Python 标准库、无外部依赖"**，提交 PR 时请务必遵循该开发规则。

---

## 📜 许可证

本项目依据 [MIT License](https://www.google.com/search?q=LICENSE) 开源。
