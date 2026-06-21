# EEWCN 数据源控制台

> [!WARNING]
> 软件目前仍处于测试阶段，无法保证软件稳定性。

> [!NOTE]
> **安全提示**：本软件会连接多个外部 WebSocket/HTTP 数据源以获取地震预警与速报，并在本机 **127.0.0.1** 开放服务端口供 EEWCN 客户端订阅。请从可信渠道下载使用。若被杀毒软件或安全软件拦截（如误报联网行为），可将本程序添加至信任名单；如有疑虑或问题，请联系我们，邮箱：jian0786@foxmail.com。

> [!NOTE]
> **软件定位**：本软件仅作为地震预警软件 **EEW CN** 的可视化控制台，用于配置、启停与管理数据源服务；**并非**独立的地震预警客户端，预警展示与推送仍由 EEW CN 完成。

## 简介

**EEWCN 数据源控制台**是 [EEWCN] 客户端的配套可视化工具：在单进程内融合地震预警（EEW）与地震速报（List）数据，对外提供本地 WebSocket / HTTP 接口，并通过图形界面启停服务、配置端口、切换上游与管理数据源开关。

配套 `custom.js` 可供 EEWCN 客户端订阅输出的预警与速报数据；端口可在「管理配置」页修改并自动同步至 custom.js。

## 功能

* EEWCN 配套图形控制台：启停「融合数据」服务、查看进程日志与健康状态
* 可配置端口（默认预警 5000、速报 8150），绑定 **127.0.0.1**，降低二次转发风险
* 地震预警 WebSocket，支持多机构预警融合推送
* 地震速报 HTTP 出口，聚合国内外速报列表
* 自定义数据源：支持单个 HTTP/HTTPS 或 WS/WSS URL，HTTP 轮询间隔可在「端口配置」页调整
* 控制台 IPC 管理：Fan Studio 主备切换、数据源开关热更新、连接统计等（无对外管理 TCP 端口）
* 可单独启用/禁用各预警与速报数据源
* CEA/JMA 上游可在 Fan Studio 与 Wolfx 之间切换
* 其他数据：BMKG、GeoNet、Early-est
* 单条共享 Fan Studio 连接，避免重复拉取
* 完整日志与磁盘缓存；支持 PyInstaller 单文件打包发布

## 本地服务端口

| 服务 | 默认端口 | 绑定地址 |
|------|----------|----------|
| 地震预警 WebSocket | 5000 | `127.0.0.1` |
| 地震速报 HTTP | 8150 | `127.0.0.1` |

端口可在控制台「管理配置」Tab 修改；修改后需重启融合服务，并会自动尝试同步 EEWCN 的 `custom.js`。

## 项目结构

```
console/          PyQt5 GUI、进程管理
services/fused/   融合核心（eew + list + common）
services/common/  开关、过滤、Fan Studio 共享连接
services/internal/  BMKG、GeoNet、Early-est、自定义源
main.py           推荐入口（GUI）
unified_console.py  兼容入口（等同 main.py GUI）
```

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

在控制台「自定义数据源」页配置 URL（留空即关闭）。支持两种 JSON 格式（平铺或 `Data` 嵌套），详见该页示例。

融合服务运行中可通过控制台 IPC 命令 `custom_data_source_url_set` **热更新** URL，无需重启；GUI 保存后也会同步。

百度翻译需自行配置环境变量 `BAIDU_APP_ID`、`BAIDU_SECRET_KEY`；未配置时地名翻译功能自动禁用并返回原文。可参考 `.env.example`。


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

## 开源声明与免责声明

为尽可能避免本项目被用于违法或恶意目的，维护者在此郑重声明如下法律立场（下列条款构成对使用者的明确约定）：

- **用途限制**：本项目仅供研究、教学、应急演练、灾害防范与减灾等合法、正当用途。任何将本项目用于违法、侵权、危害他人安全或其他恶意用途的行为均被明确禁止。
- **无担保声明**：本软件按“原样”（AS IS）提供，不对其适用性、可靠性、可用性、性能、正确性或满足特定用途作任何明示或暗示的保证，包括但不限于对适销性、特定用途适用性或不侵权的保证。
- **责任限制**：在适用法律允许的最大范围内，维护者对因使用、修改、分发或无法使用本软件而导致的任何直接、间接、附带、特殊、惩罚性或后果性损害不承担责任，即便维护者已被告知可能发生此类损害。本条款不得视为对法律强制性责任的放弃（例如在某些法域中对人身伤害或故意违法行为的责任承担）。
- **赔偿义务**：使用者应对因其使用、修改、配置或再分发本软件而导致的任何第三方索赔、损失、责任、损害或费用（包括合理的律师费）承担全部赔偿责任，并应在法律允许的范围内，使维护者免受此类索赔、损失或费用的损害。
- **遵守许可与保留声明**：任何修改、再发布或商业使用均须遵守本项目所载的 [LICENSE](LICENSE) 条款，并在分发时保留本声明、原始版权信息及许可文件。如需超出本许可的额外授权，请与维护者取得书面协议。
- **非法律意见**：本声明反映维护者对风险与责任的商业立场，不构成法律意见。如需具有法律约束力的文本或具体法律咨询，请寻求专业律师服务。

如需就免责或使用许可进行协商或签署特殊许可协议，请使用安全提示内提供的联系方式与开发者联系。

## 许可证

本项目采用 [GNU GPLv3](LICENSE) 开源协议
