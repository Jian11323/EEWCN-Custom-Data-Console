# EEWCN 数据源控制台

> [!WARNING]
> 软件目前仍处于测试阶段，无法保证软件稳定性。

> [!NOTE]
> **安全提示**：本软件会连接多个外部 WebSocket/HTTP 数据源以获取地震预警与速报，并在本机开放服务端口供客户端订阅。请从可信渠道下载使用。若被杀毒软件或安全软件拦截（如误报联网行为），可将本程序添加至信任名单；如有疑虑或问题，请联系我们，邮箱：jian0786@foxmail.com。

## 简介

自定义数据源控制台为桌面端管理工具，在单进程内融合地震预警（EEW）与地震速报（List）数据，对外提供统一的 WebSocket / HTTP 接口，并可通过图形界面启停服务、切换上游、管理数据源开关与查看运行状态。

配套 `custom.js` 可供 [EEWCN] 客户端订阅输出的预警与速报数据。

## 功能

* 图形化控制台：启停「融合数据」服务、查看进程日志与健康状态
* 地震预警 WebSocket，支持多机构预警融合推送
* 地震速报 HTTP 出口，聚合国内外速报列表
* 自定义数据源：支持单个 HTTP/HTTPS 或 WS/WSS URL
* 统一管理端口：Fan Studio 主备切换、数据源开关热更新、连接统计等
* 可单独启用/禁用各预警与速报数据源
* CEA/JMA 上游可在 Fan Studio 与 Wolfx 之间切换
* 其他数据：BMKG、GeoNet、Early-est
* 单条共享 Fan Studio 连接，避免重复拉取
* 完整日志与磁盘缓存；支持 PyInstaller 单文件打包发布

## 本地服务端口

| 服务 | 端口 |
|------|------|
| 地震预警 WebSocket | 5000 |
| 地震速报 HTTP | 8150 |
| 管理 WebSocket | 2050 |

## 启动

**图形界面（推荐）**

```bash
python main.py
```

**仅启动融合核心（无 GUI）**

```bash
python services/fused/main.py
```

## 自定义数据源

在控制台「自定义数据源」页配置 URL（留空即关闭）。支持两种 JSON 格式（平铺或 `Data` 嵌套），详见该页示例。修改后需重启「融合数据」服务。


## 数据来源

本软件汇聚以下机构或平台的数据（经 Fan Studio、Wolfx、P2PQuake 及官方接口等接入，具体以实际启用开关为准）：

* 日本气象厅地震速报：[P2PQuake API](https://www.p2pquake.net/)
* 日本地震预警（JMA）：[Wolfx WebSocket API](https://wolfx.jp/)、[FAN Studio API](https://api.fanstudio.tech/)
* 中国地震预警：[中国预警网](https://www.cea.gov.cn/)、[FAN Studio API](https://api.fanstudio.tech/)
* 中国地震速报：[中国地震台网中心](https://www.cenc.ac.cn/)、[FAN Studio API](https://api.fanstudio.tech/)
* 台湾地震预警：[FAN Studio API](https://api.fanstudio.tech/)
* 其他地震情报：[FAN Studio API](https://api.fanstudio.tech/)
* Early-est 预警：[INGV Early-est](https://early-est.rm.ingv.it/)
* 印度尼西亚地震速报：[BMKG](https://www.bmkg.go.id/)
* 新西兰地震速报：[GeoNet](https://www.geonet.org.nz/)
* 意大利地震速报：[INGV](https://www.ingv.it/)
* 用户自定义 HTTP/WS 预警源
## 开源声明

本项目为开源软件，采用 [GNU GPLv3](LICENSE) 许可证发布。

本仓库代码可供公开查阅、修改、分发和复用，但请勿将本项目用于违法、侵权或有害用途。

若本软件被他人滥用或用于违反法律法规的行为，作者不对该等行为负责；使用者应自行承担使用风险并遵守适用法律。
## 许可证

本项目采用 [GNU GPLv3](LICENSE) 开源协议
