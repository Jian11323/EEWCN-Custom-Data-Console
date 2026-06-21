from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets

from services.fused.eew.cache import CacheManager
from services.fused.eew.config import Config
from services.fused.eew.server.client_ip import ClientIPManager

class WebSocketServerManager:
    """WebSocket服务器管理器（向客户端推送数据）"""

    def __init__(self, config: Config, logger: logging.Logger, cache_mgr: CacheManager, ws_client_mgr=None, eew_service=None):
        self.config = config
        self.logger = logger
        self.cache_mgr = cache_mgr
        self.ws_client_mgr = ws_client_mgr
        self.eew_service = eew_service  # EEWService实例，用于访问线程池管理功能
        self.list_engine_module = None  # fused list engine，channel=list 时路由

        # 客户端集合（预警 WS 5000）
        self.clients_5000: Set[Any] = set()
        self.lock_5000 = threading.Lock()

        # 客户端IP管理
        self.client_ip_manager = ClientIPManager(config, logger if 'connection' in str(logger) else logger)
        
        # 广播事件循环
        self.broadcast_loop: Optional[asyncio.AbstractEventLoop] = None
        self._setup_broadcast_loop()

    def get_available_commands(self) -> Dict[str, Any]:
        """获取所有可用管理命令列表"""
        commands = {
            "stats": {
                "description": "获取连接统计信息",
                "json_command": {"command": "stats"},
                "text_commands": ["统计", "STATS", "stats"]
            },
            "history": {
                "description": "获取历史连接记录（每个IP仅保留最新一条）",
                "json_command": {"command": "history", "ip": "可选IP地址"},
                "text_commands": ["历史记录", "HISTORY", "history"]
            },
            "full_history": {
                "description": "获取完整历史连接记录（包含所有连接记录）",
                "json_command": {"command": "full_history", "ip": "可选IP地址"},
                "text_commands": ["完整历史记录", "FULL_HISTORY", "full_history"]
            },
            "ip_details": {
                "description": "获取IP连接详情",
                "json_command": {"command": "ip_details", "ip": "可选IP地址"},
                "text_commands": ["IP详情", "IP_DETAILS", "ip_details"]
            },
            "blacklist_add": {
                "description": "将IP添加到黑名单，支持时间单位：S(秒), m(分钟), h(小时), Y(年)，最低30秒，最高1年",
                "json_command": {"command": "blacklist_add", "ip": "IP地址", "duration": "时间字符串，如30S/5m/2h/1Y，或0表示永久封禁"},
                "text_commands": ["加入黑名单", "BLACKLIST_ADD", "blacklist_add"]
            },
            "blacklist_remove": {
                "description": "从黑名单移除IP",
                "json_command": {"command": "blacklist_remove", "ip": "IP地址"},
                "text_commands": ["移除黑名单", "BLACKLIST_REMOVE", "blacklist_remove"]
            },
            "blacklist_list": {
                "description": "显示黑名单中的所有IP",
                "json_command": {"command": "blacklist_list"},
                "text_commands": ["黑名单列表", "BLACKLIST_LIST", "blacklist_list"]
            },
            "fanstudio_status": {
                "description": "显示Fan Studio连接状态",
                "json_command": {"command": "fanstudio_status"},
                "text_commands": ["服务器状态", "FANSTUDIO_STATUS", "fanstudio_status"]
            },
            "fanstudio_use_backup": {
                "description": "切换至Fan Studio备用服务器并锁定，直至恢复自动切换",
                "json_command": {"command": "fanstudio_use_backup"},
                "text_commands": ["切换备用服务器", "FANSTUDIO_USE_BACKUP", "fanstudio_use_backup"]
            },
            "fanstudio_use_primary": {
                "description": "切换至Fan Studio主服务器并锁定，直至恢复自动切换",
                "json_command": {"command": "fanstudio_use_primary"},
                "text_commands": ["切换主服务器", "FANSTUDIO_USE_PRIMARY", "fanstudio_use_primary"]
            },
            "fanstudio_resume_auto": {
                "description": "清除Fan Studio手动锁定，恢复按健康度自动切换",
                "json_command": {"command": "fanstudio_resume_auto"},
                "text_commands": ["恢复FanStudio自动切换", "FANSTUDIO_RESUME_AUTO", "fanstudio_resume_auto"]
            },
            "cea_jma_wolfx": {
                "description": "CEA(CENC)+JMA 上游切换为 Wolfx all_eew（断开 Fan Studio /all；CEA_PR/CWA_FS/SA/KMA 无新数据直至切回）",
                "json_command": {"command": "cea_jma_wolfx"},
                "text_commands": ["切换wolfx服务器", "WOLFX_UPSTREAM", "wolfx_upstream", "cea_jma_wolfx"]
            },
            "cea_jma_fanstudio": {
                "description": "CEA+JMA 上游切回 Fan Studio /all（断开 Wolfx，恢复全量 Fan 源）",
                "json_command": {"command": "cea_jma_fanstudio"},
                "text_commands": ["切换fan studio服务器", "CEA_JMA_FANSTUDIO_UPSTREAM", "cea_jma_fanstudio"]
            },
            "set_connection_limits": {
                "description": "设置连接数限制参数",
                "json_command": {"command": "set_connection_limits", "max_connections": 20, "timeout": 1800},
                "text_commands": ["设置连接限制", "SET_CONNECTION_LIMITS", "set_connection_limits"]
            },
            "auto_check": {
                "description": "自动检查所有模块状态",
                "json_command": {"command": "auto_check"},
                "text_commands": ["自动检查", "AUTO_CHECK", "auto_check"]
            },
            "source_switches_get": {
                "description": "获取数据源开关状态",
                "json_command": {"command": "source_switches_get", "channel": "eew"},
                "text_commands": ["数据源开关", "SOURCE_SWITCHES_GET", "source_switches_get"]
            },
            "source_switches_set": {
                "description": "设置数据源开关（热更新）",
                "json_command": {"command": "source_switches_set", "channel": "eew", "patch": {"CUSTOM": True}},
                "text_commands": ["设置数据源开关", "SOURCE_SWITCHES_SET", "source_switches_set"]
            },
            "source_filters_get": {
                "description": "获取国外源阈值与地区过滤配置",
                "json_command": {"command": "source_filters_get"},
                "text_commands": ["SOURCE_FILTERS_GET", "source_filters_get"]
            },
            "source_filters_set": {
                "description": "设置国外源阈值与地区过滤（热更新）",
                "json_command": {
                    "command": "source_filters_set",
                    "list_source_threshold": {"usgs": 4.5},
                },
                "text_commands": ["SOURCE_FILTERS_SET", "source_filters_set"]
            },
            "thread_pool_status": {
                "description": "获取线程池运行状态",
                "json_command": {"command": "thread_pool_status"},
                "text_commands": ["线程池实况", "THREAD_POOL_STATUS", "thread_pool_status"]
            },
            "thread_pool_check": {
                "description": "执行线程池健康检查",
                "json_command": {"command": "thread_pool_check"},
                "text_commands": ["线程池检查", "THREAD_POOL_CHECK", "thread_pool_check"]
            },
            "thread_pool_restart": {
                "description": "重启线程池",
                "json_command": {"command": "thread_pool_restart"},
                "text_commands": ["线程池重启", "THREAD_POOL_RESTART", "thread_pool_restart"]
            },
            "all_commands": {
                "description": "显示所有可用管理命令",
                "json_command": {"command": "all_commands"},
                "text_commands": ["全部命令", "ALL_COMMANDS", "all_commands", "命令列表", "帮助", "help"]
            },
            "logout": {
                "description": "退出管理员模式",
                "json_command": {"type": "logout"},
                "text_commands": ["退出", "LOGOUT", "logout", "exit"]
            }
        }
        return commands

    def _resolve_plain_management_command(self, text: str) -> Tuple[str, Dict[str, Any]]:
        """解析管理端口纯文本为 (command, params)。

        已注册的完整文本命令（含空格，如「切换fan studio服务器」）按整行匹配；
        否则按首词为命令、其余为 args（如「加入黑名单 1.2.3.4 30m」）。
        整行匹配使用 casefold，以兼容「切换Fan studio服务器」等大小写变体。
        """
        stripped = (text or "").strip()
        if not stripped:
            return "", {"args": []}
        cf_to_canonical: Dict[str, str] = {}
        for cmd_info in self.get_available_commands().values():
            for tc in cmd_info.get("text_commands", ()):
                ck = tc.casefold()
                if ck not in cf_to_canonical:
                    cf_to_canonical[ck] = tc
        whole_cf = stripped.casefold()
        if whole_cf in cf_to_canonical:
            return cf_to_canonical[whole_cf], {"args": []}
        parts = stripped.split()
        cmd = parts[0] if parts else ""
        return cmd, {"args": parts[1:] if len(parts) > 1 else []}

    async def _send_available_commands(self, websocket, is_json: bool = True):
        """向客户端发送可用命令列表"""
        try:
            commands = self.get_available_commands()

            if is_json:
                # JSON格式：发送结构化的命令列表
                await websocket.send(json.dumps({
                    "type": "available_commands",
                    "message": "以下是可用的管理命令：",
                    "commands": commands
                }))
            else:
                # 纯文本格式：发送格式化的文本列表
                response = "=== 可用管理命令 ===\n\n"
                for cmd_key, cmd_info in commands.items():
                    response += f"{cmd_info['description']}:\n"
                    response += f"  文本命令: {', '.join(cmd_info['text_commands'])}\n"
                    response += f"  JSON示例: {cmd_info['json_command']}\n\n"

                response += "提示：发送对应命令即可执行，JSON格式使用 {\"command\": \"命令名\"} 结构"
                await websocket.send(response)

        except Exception as e:
            self.logger.debug(f"发送可用命令列表失败: {e}")

    def _setup_broadcast_loop(self):
        from services.fused.eew.server.broadcast import setup_broadcast_loop
        setup_broadcast_loop(self)

    async def handle_client(self, websocket, port: int):
        from services.fused.eew.server.broadcast import handle_client as _handle_client
        await _handle_client(self, websocket, port)

    async def broadcast_async(self, message: str, port: int):
        from services.fused.eew.server.broadcast import broadcast_async as _broadcast_async
        await _broadcast_async(self, message, port)

    def broadcast(self, message: str, port: int):
        from services.fused.eew.server.broadcast import broadcast as _broadcast
        _broadcast(self, message, port)

    def start_server(self, port: int):
        from services.fused.eew.server.broadcast import start_ws_server
        start_ws_server(self, port)

    def set_list_engine_module(self, fl_module) -> None:
        self.list_engine_module = fl_module

    async def _send_mgmt_json(self, websocket, payload: dict, **json_kw) -> None:
        from services.common.mgmt_locale import localize_mgmt_envelope
        kw = {"ensure_ascii": False, "default": str}
        kw.update(json_kw)
        await websocket.send(json.dumps(localize_mgmt_envelope(payload), **kw))

    _MGMT_FANSTUDIO_COMMANDS = frozenset({
        "fanstudio_status", "服务器状态", "FANSTUDIO_STATUS",
        "fanstudio_use_backup", "切换备用服务器", "切换副服务器", "FANSTUDIO_USE_BACKUP",
        "fanstudio_use_primary", "切换主服务器", "FANSTUDIO_USE_PRIMARY",
        "fanstudio_resume_auto", "恢复FanStudio自动切换", "FANSTUDIO_RESUME_AUTO",
    })

    async def _execute_list_channel(self, websocket, command, params, is_json: bool) -> None:
        if not self.list_engine_module:
            msg = "List 模块未挂载"
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
            else:
                await websocket.send(f"ERROR:{msg}")
            return
        from services.fused.list.management_context import (
            ListManagementContext,
            execute_list_command_with_context,
        )
        ctx = ListManagementContext.from_module(self.list_engine_module)
        try:
            result = await asyncio.to_thread(
                execute_list_command_with_context, command, ctx, params,
            )
            if is_json:
                await self._send_mgmt_json(websocket, {
                    "type": "result", "command": command, "channel": "list", "data": result,
                })
            else:
                await websocket.send(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        except ValueError as e:
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": str(e)})
            else:
                await websocket.send(f"ERROR:{str(e)}")

    async def _execute_both_channel(self, websocket, command, params, is_json: bool) -> None:
        if command in self._MGMT_FANSTUDIO_COMMANDS:
            eew_params = {k: v for k, v in params.items() if k != "channel"}
            await self._execute_management_command(websocket, command, eew_params, is_json)
            return
        if command in ("stats", "统计", "STATS"):
            eew_stats = self.client_ip_manager.get_connection_stats()
            from services.fused.list.management_context import (
                ListManagementContext,
                execute_list_command_with_context,
            )
            ctx = ListManagementContext.from_module(self.list_engine_module)
            list_stats = await asyncio.to_thread(
                execute_list_command_with_context, "stats", ctx, {},
            )
            data = {"eew": eew_stats, "list": list_stats}
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "stats", "channel": "both", "data": data})
            else:
                await websocket.send(json.dumps(data, ensure_ascii=False, indent=2))
            return
        if command in ("auto_check", "自动检查", "AUTO_CHECK"):
            eew_check = await self._perform_auto_check()
            from services.fused.list.management_context import (
                ListManagementContext,
                execute_list_command_with_context,
            )
            ctx = ListManagementContext.from_module(self.list_engine_module)
            list_check = await asyncio.to_thread(
                execute_list_command_with_context, "auto_check", ctx, {},
            )
            data = {"eew": eew_check, "list": list_check}
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "auto_check", "channel": "both", "data": data})
            else:
                await websocket.send(self._format_check_result_text(eew_check) + "\n\n--- List ---\n" + json.dumps(list_check, ensure_ascii=False, indent=2))
            return
        if command in ("source_status", "SOURCE_STATUS", "数据源状态", "各数据源采集状态"):
            from services.common.source_status import get_source_status_registry
            snap = get_source_status_registry().snapshot()
            if self.list_engine_module:
                snap["list_error_stats"] = dict(getattr(self.list_engine_module, "error_stats", {}))
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "source_status", "channel": "both", "data": snap})
            else:
                lines = ["=== 数据源采集状态 ==="]
                for sid, info in snap.get("sources", {}).items():
                    conn = "已连接" if info.get("connected") else "断开"
                    lines.append(f"{info.get('label', sid)} [{sid}]: {conn}")
                await websocket.send("\n".join(lines))
            return
        msg = f"channel=both 不支持命令: {command}"
        if is_json:
            await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
        else:
            await websocket.send(f"ERROR:{msg}")

    async def _execute_management_command(self, websocket, command, params, is_json=True, is_admin2580=False):
        """执行管理命令"""
        try:
            channel = "eew"
            if is_json:
                channel = (params.get("channel") or "eew").lower()
            if channel == "list":
                await self._execute_list_channel(websocket, command, params, is_json)
                return
            if channel == "both":
                await self._execute_both_channel(websocket, command, params, is_json)
                return

            # 获取参数
            if is_json:
                ip = params.get('ip')
                enabled = params.get('enabled', False)
            else:
                args = params.get('args', [])
                ip = args[0] if args else None
                enabled = len(args) > 0 and args[0].lower() in ('true', '1', 'enable', 'enabled')

            # 执行命令
            if command in ('统计', 'STATS', 'stats'):
                # 获取连接统计信息 - 显示所有客户端IP的连接情况
                stats = self.client_ip_manager.get_connection_stats()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "stats", "data": stats})
                else:
                    # 纯文本格式化输出
                    response = "=== 连接统计 ===\n"
                    response += f"总IP数: {stats.get('总IP数', 'N/A')}\n"
                    response += f"活跃IP数: {stats.get('活跃IP数', 'N/A')}\n"
                    response += f"总连接数: {stats.get('总连接数', 'N/A')}\n"
                    response += f"黑名单IP数: {stats.get('黑名单IP数', 'N/A')}\n"
                    response += f"每IP最大连接数: {stats.get('每IP最大连接数', 'N/A')}"
                    await websocket.send(response)

            elif command in ('历史记录', 'HISTORY', 'history'):
                # 获取历史连接记录 - 显示已断开的IP连接信息（每个IP仅保留最新一条）
                if is_json:
                    ip_filter = params.get('ip')
                else:
                    args = params.get('args', [])
                    ip_filter = args[0] if args else None

                history_raw = self.client_ip_manager.get_connection_history(ip_filter)

                # 在管理端口返回前，把时间戳统一格式化为 YYYY/MM/DD HH:MM:SS
                def _fmt(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                history = []
                for item in history_raw:
                    new_item = dict(item)
                    if isinstance(new_item.get("首次连接时间"), (int, float)):
                        new_item["首次连接时间"] = _fmt(new_item["首次连接时间"])
                    if isinstance(new_item.get("最后活动时间"), (int, float)):
                        new_item["最后活动时间"] = _fmt(new_item["最后活动时间"])
                    if isinstance(new_item.get("断开时间"), (int, float)):
                        new_item["断开时间"] = _fmt(new_item["断开时间"])
                    history.append(new_item)

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "history", "data": history})
                else:
                    if not history:
                        response = "=== 历史连接记录 ===\n(无历史记录)\n"
                    else:
                        response_lines = ["=== 历史连接记录 ==="]
                        # 按断开时间倒序显示，最近断开的在前
                        for item in sorted(history, key=lambda x: x.get("断开时间", 0), reverse=True):
                            ip_addr = item.get("IP地址", "未知IP")
                            first_seen = item.get("首次连接时间", "N/A")
                            last_seen = item.get("最后活动时间", "N/A")
                            disconnected_at = item.get("断开时间", "N/A")
                            ports = item.get("连接端口", [])
                            response_lines.append(f"IP: {ip_addr}")
                            response_lines.append(f"  首次连接时间: {first_seen}")
                            response_lines.append(f"  最后活动时间: {last_seen}")
                            response_lines.append(f"  断开时间: {disconnected_at}")
                            response_lines.append(f"  连接端口: {ports}")
                            response_lines.append("")  # 空行分隔
                        response = "\n".join(response_lines)

                    await websocket.send(response)

            elif command in ('完整历史记录', 'FULL_HISTORY', 'full_history'):
                # 获取完整历史连接记录 - 从独立文件中读取所有连接记录
                if is_json:
                    ip_filter = params.get('ip')
                else:
                    args = params.get('args', [])
                    ip_filter = args[0] if args else None

                history_raw = self.client_ip_manager.load_full_history(ip_filter)

                # 在管理端口返回前，把时间戳统一格式化为 YYYY/MM/DD HH:MM:SS
                def _fmt_full(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                history = []
                for item in history_raw:
                    new_item = dict(item)
                    if isinstance(new_item.get("首次连接时间"), (int, float)):
                        new_item["首次连接时间"] = _fmt_full(new_item["首次连接时间"])
                    if isinstance(new_item.get("最后活动时间"), (int, float)):
                        new_item["最后活动时间"] = _fmt_full(new_item["最后活动时间"])
                    if isinstance(new_item.get("断开时间"), (int, float)):
                        new_item["断开时间"] = _fmt_full(new_item["断开时间"])
                    history.append(new_item)

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "full_history", "data": history})
                else:
                    if not history:
                        response = "=== 完整历史记录 ===\n(无历史记录)\n"
                    else:
                        response_lines = ["=== 完整历史记录 ==="]
                        # 按断开时间倒序显示，最近断开的在前
                        for item in sorted(history, key=lambda x: x.get("断开时间", 0), reverse=True):
                            ip_addr = item.get("IP地址", "未知IP")
                            first_seen = item.get("首次连接时间", "N/A")
                            last_seen = item.get("最后活动时间", "N/A")
                            disconnected_at = item.get("断开时间", "N/A")
                            ports = item.get("连接端口", [])
                            response_lines.append(f"IP: {ip_addr}")
                            response_lines.append(f"  首次连接时间: {first_seen}")
                            response_lines.append(f"  最后活动时间: {last_seen}")
                            response_lines.append(f"  断开时间: {disconnected_at}")
                            response_lines.append(f"  连接端口: {ports}")
                            response_lines.append("")  # 空行分隔
                        response = "\n".join(response_lines)

                    await websocket.send(response)

            elif command in ('IP详情', 'IP_DETAILS', 'ip_details'):
                details_raw = self.client_ip_manager.get_ip_details(ip)

                # 仅在管理端口返回时格式化时间
                def _fmt(ts):
                    try:
                        return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")
                    except Exception:
                        return ts

                if ip:
                    details = dict(details_raw) if details_raw else {}
                    if isinstance(details.get('first_seen'), (int, float)):
                        details['first_seen'] = _fmt(details['first_seen'])
                    if isinstance(details.get('last_seen'), (int, float)):
                        details['last_seen'] = _fmt(details['last_seen'])
                else:
                    # 所有IP时，保持结构不变，只在文本输出里做格式化
                    details = details_raw if details_raw else {}

                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "ip_details", "data": details})
                else:
                    if ip and details:
                        response = f"=== IP {ip} 详情 ===\n"
                        first_seen = details.get('first_seen', 'N/A')
                        last_seen = details.get('last_seen', 'N/A')
                        if isinstance(first_seen, (int, float)):
                            first_seen = _fmt(first_seen)
                        if isinstance(last_seen, (int, float)):
                            last_seen = _fmt(last_seen)
                        response += f"连接数: {details.get('connections', 0)}\n"
                        response += f"首次连接: {first_seen}\n"
                        response += f"最后连接: {last_seen}\n"
                        response += f"连接端口: {list(details.get('ports', []))}"
                    elif not ip:
                        response = "=== 所有IP详情 ===\n"
                        # 按端口分类显示IP
                        port_ips = {5000: []}
                        # 确保details是字典类型
                        if isinstance(details, dict):
                            for ip_addr, info in details.items():
                                if isinstance(info, dict):
                                    ports = info.get('ports', set())
                                    # ports可能是set或list，统一处理
                                    if isinstance(ports, set):
                                        ports = list(ports)
                                    for port in ports:
                                        if port in port_ips:
                                            port_ips[port].append(ip_addr)

                        for port in [5000]:
                            response += f"[{port}]端口\n"
                            if port_ips[port]:
                                for ip_addr in sorted(port_ips[port]):
                                    response += f"{ip_addr}\n"
                            else:
                                response += "(无连接)\n"
                            response += "\n"  # 端口间空行分隔
                    else:
                        response = f"IP {ip} 未找到"
                    await websocket.send(response)

            elif command in ('加入黑名单', 'BLACKLIST_ADD', 'blacklist_add'):
                try:
                    if is_json:
                        ip = params.get('ip')
                        duration = params.get('duration', 0)  # 默认为永久封禁，支持字符串格式如 "30S", "5m", "2h", "1Y"
                    else:
                        args = params.get('args', [])
                        ip = args[0] if len(args) > 0 else None
                        # 第二个参数是时间，可能是字符串格式如 "30S" 或数字
                        if len(args) > 1:
                            duration_str = str(args[1])
                            # 尝试解析为整数（兼容旧格式）
                            try:
                                duration = int(duration_str)
                            except ValueError:
                                # 如果不是纯数字，当作时间字符串处理
                                duration = duration_str
                        else:
                            duration = 0

                    if ip:
                        self.client_ip_manager.add_to_blacklist(ip, duration)
                        # 断开该IP的所有现有连接
                        disconnected_count = await self.disconnect_ip(ip)
                        
                        # 格式化消息
                        if duration == 0 or duration is None:
                            msg = f"IP {ip} 已添加到黑名单，永久封禁"
                        else:
                            # 显示原始输入的时间格式
                            if isinstance(duration, str):
                                msg = f"IP {ip} 已添加到黑名单，封禁 {duration}"
                            else:
                                # 兼容旧格式：如果是数字，显示为分钟
                                if duration > 10000:
                                    msg = f"IP {ip} 已添加到黑名单，封禁 {duration} 秒"
                                else:
                                    msg = f"IP {ip} 已添加到黑名单，封禁 {duration} 分钟"
                        
                        if disconnected_count > 0:
                            msg += f"，已断开 {disconnected_count} 个现有连接"
                    else:
                        msg = "需要指定IP地址"
                    
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": bool(ip), "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                except ValueError as e:
                    error_msg = f"添加黑名单失败: {str(e)}"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
                    else:
                        await websocket.send(f"ERROR:{error_msg}")
                except Exception as e:
                    error_msg = f"添加黑名单时发生错误: {str(e)}"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
                    else:
                        await websocket.send(f"ERROR:{error_msg}")

            elif command in ('移除黑名单', 'BLACKLIST_REMOVE', 'blacklist_remove'):
                if ip:
                    self.client_ip_manager.remove_from_blacklist(ip)
                    msg = f"IP {ip} 已从黑名单移除，现在可以正常连接"
                else:
                    msg = "需要指定IP地址"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "result", "success": bool(ip), "message": msg})
                else:
                    await websocket.send(f"RESULT:{msg}")

            elif command in ('黑名单列表', 'BLACKLIST_LIST', 'blacklist_list'):
                blacklist = self.client_ip_manager.blacklist
                if is_json:
                    # 转换格式：包含过期时间信息
                    data = {}
                    for ip, expiry in blacklist.items():
                        if expiry == 0:
                            data[ip] = {"type": "permanent"}
                        else:
                            remaining_minutes = max(0, int((expiry - time.time()) / 60))
                            data[ip] = {"type": "temporary", "remaining_minutes": remaining_minutes}
                    await self._send_mgmt_json(websocket, {"type": "blacklist_list", "data": data})
                else:
                    if blacklist:
                        response = "=== 黑名单列表 ===\n"
                        for ip in sorted(blacklist.keys()):
                            expiry = blacklist[ip]
                            if expiry == 0:
                                response += f"• {ip} (永久封禁)\n"
                            else:
                                remaining_minutes = max(0, int((expiry - time.time()) / 60))
                                response += f"• {ip} (剩余 {remaining_minutes} 分钟)\n"
                        response = response.rstrip()  # 移除最后的换行符
                    else:
                        response = "黑名单为空"
                    await websocket.send(response)

            elif command in ('source_status', 'SOURCE_STATUS', '数据源状态'):
                from services.common.source_status import get_source_status_registry
                snap = get_source_status_registry().snapshot()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_status", "data": snap})
                else:
                    lines = ["=== 数据源采集状态 ==="]
                    for sid, info in snap.get("sources", {}).items():
                        conn = "已连接" if info.get("connected") else "断开"
                        lines.append(f"{info.get('label', sid)} [{sid}]: {conn}, 消息数={info.get('message_count', 0)}")
                    await websocket.send("\n".join(lines))

            elif command in ('服务器状态', 'FANSTUDIO_STATUS', 'fanstudio_status'):
                if self.ws_client_mgr:
                    status = {
                        'current_server': self.ws_client_mgr.current_server_url,
                        'manual_target': self.ws_client_mgr.fanstudio_manual_target,
                        'cea_jma_upstream': self.ws_client_mgr.cea_jma_upstream,
                        'wolfx_url': self.ws_client_mgr.config.WOLFX_ALL_EEW_URL,
                        'primary_health': self.ws_client_mgr.connection_health['primary_server_health'],
                        'backup_health': self.ws_client_mgr.connection_health['backup_server_health'],
                        'connection_quality': self.ws_client_mgr.connection_health['connection_quality'],
                        'fail_streak': self.ws_client_mgr.connection_stats['current_fail_streak'],
                        'server_switches': self.ws_client_mgr.connection_stats['server_switch_count']
                    }
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "fanstudio_status", "data": status})
                    else:
                        response = "=== Fan Studio连接状态 ===\n"
                        response += f"当前服务器: {status['current_server']}\n"
                        response += f"CEA/JMA 上游: {status.get('cea_jma_upstream', 'fanstudio')}（wolfx 时见 Wolfx URL）\n"
                        if status.get('cea_jma_upstream') == 'wolfx':
                            response += f"Wolfx: {status.get('wolfx_url', '')}\n"
                        mt = status.get('manual_target')
                        if mt:
                            response += f"手动锁定: {mt}（自动切换已暂停，请发恢复FanStudio自动切换）\n"
                        else:
                            response += "手动锁定: 无（自动切换已启用）\n"
                        response += f"主服务器健康度: {status['primary_health']:.2f}\n"
                        response += f"备用服务器健康度: {status['backup_health']:.2f}\n"
                        response += f"连接质量: {status['connection_quality']}\n"
                        response += f"连续失败次数: {status['fail_streak']}\n"
                        response += f"服务器切换次数: {status['server_switches']}"
                        await websocket.send(response)
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换wolfx服务器', 'WOLFX_UPSTREAM', 'wolfx_upstream', 'cea_jma_wolfx'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_cea_jma_to_wolfx)
                    msg = (
                        "已切换 CEA/JMA 至 Wolfx all_eew，Fan Studio /all 已断开；"
                        "CEA_PR/CWA_FS/SA/KMA 等无推送直至发送「切换fan studio服务器」"
                    )
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换fan studio服务器', 'CEA_JMA_FANSTUDIO_UPSTREAM', 'cea_jma_fanstudio'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_cea_jma_to_fanstudio)
                    msg = "已切换 CEA/JMA 回 Fan Studio /all，Wolfx 已断开，Fan Studio 将重连"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换备用服务器', '切换副服务器', 'FANSTUDIO_USE_BACKUP', 'fanstudio_use_backup'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_fanstudio_to_backup)
                    msg = "已切换至 Fan Studio 备用服务器并锁定，自动切换已暂停；需恢复请发送 fanstudio_resume_auto / 恢复FanStudio自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('切换主服务器', 'FANSTUDIO_USE_PRIMARY', 'fanstudio_use_primary'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.switch_fanstudio_to_primary)
                    msg = "已切换至 Fan Studio 主服务器并锁定，自动切换已暂停；需恢复请发送 fanstudio_resume_auto / 恢复FanStudio自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('恢复FanStudio自动切换', 'FANSTUDIO_RESUME_AUTO', 'fanstudio_resume_auto'):
                if self.ws_client_mgr:
                    await asyncio.to_thread(self.ws_client_mgr.fanstudio_resume_auto_switch)
                    msg = "已清除 Fan Studio 手动锁定，恢复按健康度自动切换"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                    else:
                        await websocket.send(f"RESULT:{msg}")
                else:
                    msg = "WebSocket 客户端管理器不可用"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('设置连接限制', 'SET_CONNECTION_LIMITS', 'set_connection_limits'):
                if is_json:
                    max_connections = params.get('max_connections', 20)
                    timeout = params.get('timeout', 1800)
                else:
                    # 从文本命令解析参数
                    args = params.get('args', [])
                    max_connections = int(args[0]) if len(args) > 0 else 20
                    timeout = int(args[1]) if len(args) > 1 else 1800

                # 更新设置
                old_max = self.client_ip_manager.max_connections_per_ip
                old_timeout = self.client_ip_manager.connection_timeout

                self.client_ip_manager.max_connections_per_ip = max_connections
                self.client_ip_manager.connection_timeout = timeout

                msg = f"连接限制已更新: 最大连接数 {old_max} -> {max_connections}, 超时时间 {old_timeout} -> {timeout}秒"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "result", "success": True, "message": msg})
                else:
                    await websocket.send(f"RESULT:{msg}")

            elif command in ('自动检查', 'AUTO_CHECK', 'auto_check'):
                # 执行自动检查
                check_result = await self._perform_auto_check()
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "auto_check", "data": check_result})
                else:
                    response = self._format_check_result_text(check_result)
                    await websocket.send(response)

            elif command in ('线程池实况', 'THREAD_POOL_STATUS', 'thread_pool_status'):
                # 获取线程池运行状态
                if self.eew_service:
                    status = self.eew_service.get_thread_pool_status()
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "thread_pool_status", "data": status})
                    else:
                        response = "=== 线程池运行状态 ===\n"
                        response += f"状态: {status.get('状态', 'N/A')}\n"
                        response += f"最大工作线程数: {status.get('最大工作线程数', 'N/A')}\n"
                        response += f"活动线程数: {status.get('活动线程数', 'N/A')}\n"
                        response += f"队列大小: {status.get('队列大小', 'N/A')}\n"
                        response += f"创建时间: {status.get('创建时间', 'N/A')}\n"
                        response += f"运行时间: {status.get('运行时间', 'N/A')}\n"
                        response += f"总任务数: {status.get('总任务数', 'N/A')}"
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法获取线程池状态"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('线程池检查', 'THREAD_POOL_CHECK', 'thread_pool_check'):
                # 执行线程池健康检查
                if self.eew_service:
                    check_result = self.eew_service.check_thread_pool()
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "thread_pool_check", "data": check_result})
                    else:
                        response = "=== 线程池健康检查 ===\n"
                        response += f"健康状态: {check_result.get('健康状态', 'N/A')}\n"
                        response += f"检查时间: {check_result.get('时间戳', 'N/A')}\n\n"
                        
                        status = check_result.get('状态', {})
                        response += "运行状态:\n"
                        response += f"  状态: {status.get('状态', 'N/A')}\n"
                        response += f"  最大工作线程数: {status.get('最大工作线程数', 'N/A')}\n"
                        response += f"  活动线程数: {status.get('活动线程数', 'N/A')}\n"
                        response += f"  队列大小: {status.get('队列大小', 'N/A')}\n"
                        response += f"  运行时间: {status.get('运行时间', 'N/A')}\n"
                        response += f"  总任务数: {status.get('总任务数', 'N/A')}\n\n"
                        
                        issues = check_result.get('异常问题', [])
                        if issues:
                            response += "异常问题:\n"
                            for issue in issues:
                                response += f"  ⚠️ {issue}\n"
                            response += "\n"
                        
                        warnings = check_result.get('警告信息', [])
                        if warnings:
                            response += "警告信息:\n"
                            for warning in warnings:
                                response += f"  ⚡ {warning}\n"
                        
                        if not issues and not warnings:
                            response += "✓ 线程池运行正常，无异常"
                        
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法执行线程池检查"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('线程池重启', 'THREAD_POOL_RESTART', 'thread_pool_restart'):
                # 重启线程池（后台执行），避免阻塞管理端口
                if self.eew_service:
                    # 在独立线程中执行实际的重启逻辑，管理端口仅负责下发指令
                    def _restart_worker():
                        result = self.eew_service.restart_thread_pool()
                        logger = self.eew_service.log_mgr.get_logger('data')
                        logger.info(f"[管理命令] 线程池重启任务完成: {result.get('消息', 'N/A')}")
                    threading.Thread(target=_restart_worker, daemon=True, name="ThreadPool-Restart").start()

                    if is_json:
                        # 立即返回“已下发”状态，重启过程在后台执行
                        await self._send_mgmt_json(websocket, {
                            "type": "thread_pool_restart",
                            "data": {
                                "started": True,
                                "message": "线程池重启命令已下发，重启过程在后台执行，请稍后通过 thread_pool_status / thread_pool_check 查询最新状态"
                            },
                        })
                    else:
                        response = "=== 线程池重启命令已下发 ===\n"
                        response += "重启将在后台执行，管理端口不会被阻塞。\n"
                        response += "请稍后通过「线程池实况」或「线程池检查」查看最新状态。"
                        await websocket.send(response)
                else:
                    msg = "EEWService不可用，无法重启线程池"
                    if is_json:
                        await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                    else:
                        await websocket.send(f"ERROR:{msg}")

            elif command in ('数据源开关', 'SOURCE_SWITCHES_GET', 'source_switches_get'):
                from services.common.source_switches import get_registry, EEW_SOURCE_NAMES, LIST_SOURCE_NAMES
                ch = params.get('channel', 'eew') if is_json else 'eew'
                snap = get_registry(ch).snapshot()
                names = EEW_SOURCE_NAMES if ch == 'eew' else LIST_SOURCE_NAMES
                payload = {"channel": ch, "switches": snap, "names": names}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_switches", "data": payload})
                else:
                    lines = [f"=== 数据源开关 ({ch}) ==="]
                    for k, v in snap.items():
                        lines.append(f"  {k}: {'开' if v else '关'}")
                    await websocket.send("\n".join(lines))

            elif command in ('设置数据源开关', 'SOURCE_SWITCHES_SET', 'source_switches_set'):
                from services.common.source_switches import apply_eew_patch, save_to_settings_path
                patch = params.get('patch', {}) if is_json else {}
                if not patch and is_json:
                    patch = {k: v for k, v in params.items() if k not in ('command', 'channel') and isinstance(v, bool)}
                apply_eew_patch(patch) if patch else []
                evicted = []
                if self.eew_service and self.eew_service.distributor:
                    dist = self.eew_service.distributor
                    for sid, enabled in (patch or {}).items():
                        if enabled is False:
                            dist.evict_source(sid)
                            evicted.append(sid)
                save_to_settings_path()
                result = {
                    "ok": True,
                    "patch": patch,
                    "disabled_by_mutex": [],
                    "evicted": list(set(evicted)),
                    "republished": [],
                }
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_switches_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in (
                '设置自定义数据源URL',
                'CUSTOM_DATA_SOURCE_URL_SET',
                'custom_data_source_url_set',
            ):
                from services.common.source_switches import (
                    is_eew_enabled,
                    set_custom_data_source_url,
                )
                from services.internal import custom as custom_internal
                url = params.get('url', '') if is_json else ''
                set_custom_data_source_url(url)
                started = False
                if url and is_eew_enabled('CUSTOM'):
                    thread = custom_internal.start()
                    started = thread is not None and thread.is_alive()
                else:
                    custom_internal.stop()
                result = {"ok": True, "url": url, "started": started}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "custom_data_source_url_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in ('SOURCE_FILTERS_GET', 'source_filters_get'):
                from services.common.source_filters import get_filter_registry
                payload = {"filters": get_filter_registry().snapshot()}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_filters", "data": payload})
                else:
                    await websocket.send(json.dumps(payload, ensure_ascii=False, indent=2))

            elif command in (
                'HTTP_POLL_INTERVALS_SET',
                'http_poll_intervals_set',
                '设置HTTP轮询间隔',
            ):
                from services.common.http_poll_intervals import get_all_intervals, set_poll_intervals
                intervals = params.get('intervals', {}) if is_json else {}
                if not isinstance(intervals, dict):
                    intervals = {}
                saved = set_poll_intervals(intervals) if intervals else get_all_intervals()
                result = {"ok": True, "intervals": saved}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "http_poll_intervals_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in ('SOURCE_FILTERS_SET', 'source_filters_set'):
                from services.common.source_filters import get_filter_registry, save_to_settings_path
                reg = get_filter_registry()
                reg.apply_patch(
                    list_threshold=params.get('list_source_threshold'),
                    list_region_filter=params.get('list_source_region_filter'),
                    eew_threshold=params.get('eew_source_threshold'),
                    eew_region_filter=params.get('eew_source_region_filter'),
                )
                save_to_settings_path()
                result = {"ok": True, "filters": reg.snapshot()}
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "source_filters_set", "data": result})
                else:
                    await websocket.send(json.dumps(result, ensure_ascii=False, indent=2))

            elif command in ('全部命令', 'ALL_COMMANDS', 'all_commands', '命令列表', '帮助', 'help'):
                # 发送所有可用命令列表
                await self._send_available_commands(websocket, is_json)

            else:
                msg = f"未知命令: {command}"
                if is_json:
                    await self._send_mgmt_json(websocket, {"type": "error", "message": msg})
                else:
                    await websocket.send(f"ERROR:{msg}")

        except Exception as e:
            error_msg = str(e)
            if is_json:
                await self._send_mgmt_json(websocket, {"type": "error", "message": error_msg})
            else:
                await websocket.send(f"ERROR:{error_msg}")

    async def disconnect_ip(self, target_ip: str):
        """断开指定IP的所有连接"""
        disconnected_count = 0

        # 检查所有端口的客户端集合
        client_sets = [(self.clients_5000, self.lock_5000, 5000)]

        for clients, lock, port in client_sets:
            with lock:
                # 收集需要断开的websocket连接
                to_disconnect = []
                for ws in clients:
                    try:
                        client_addr = ws.remote_address
                        client_ip = client_addr[0] if client_addr else 'unknown'
                        if client_ip == target_ip:
                            to_disconnect.append(ws)
                    except Exception as e:
                        self.logger.debug(f"获取客户端IP失败: {e}")

                # 断开连接并从集合中移除
                for ws in to_disconnect:
                    try:
                        await ws.close(code=1008, reason="Access denied - IP blacklisted")
                        clients.discard(ws)
                        # 记录断开连接，减少连接计数
                        self.client_ip_manager.record_disconnection(target_ip, port)
                        disconnected_count += 1
                        self.logger.info(f"[端口{port}] 断开黑名单IP连接: {target_ip}")
                    except Exception as e:
                        self.logger.debug(f"断开连接失败: {e}")

        if disconnected_count > 0:
            self.logger.info(f"已断开IP {target_ip} 的 {disconnected_count} 个连接")
        else:
            self.logger.debug(f"IP {target_ip} 没有找到活跃连接")

        return disconnected_count

    async def _perform_auto_check(self) -> Dict[str, Any]:
        """执行自动检查所有模块"""
        check_result = {
            "timestamp": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
            "modules": {},
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "warnings": 0
            }
        }

        # 1. 检查数据源连接状态
        data_sources_status = {}
        if self.ws_client_mgr and self.ws_client_mgr.sources:
            from services.common.source_status import get_source_status_registry
            reg_sources = get_source_status_registry().snapshot().get("sources", {})
            if hasattr(self.ws_client_mgr, "_sync_internal_source_connected"):
                self.ws_client_mgr._sync_internal_source_connected()
            for source_key, source in self.ws_client_mgr.sources.items():
                try:
                    source_name = DataSource.SOURCE_NAME_MAP.get(source_key, source_key)
                    
                    is_connected = DataSource.resolve_connected(source_key, source, reg_sources)
                    status = "正常" if is_connected else "未连接"
                    
                    extra_info: Dict[str, Any] = {}
                    ds_entry = {
                        "status": status,
                        "连接状态": "已连接" if is_connected else "未连接",
                        "result": "通过" if is_connected else "警告",
                    }
                    ds_entry.update(extra_info)
                    
                    data_sources_status[source_name] = ds_entry
                    check_result["summary"]["total"] += 1
                    if is_connected:
                        check_result["summary"]["passed"] += 1
                    else:
                        check_result["summary"]["warnings"] += 1
                except Exception as e:
                    data_sources_status[source_key] = {
                        "status": "检查失败",
                        "error": str(e),
                        "result": "失败"
                    }
                    check_result["summary"]["total"] += 1
                    check_result["summary"]["failed"] += 1

        check_result["modules"]["数据源"] = data_sources_status

        # 2. 检查WebSocket服务器状态
        from services.common.ports import get_eew_port

        eew_port = get_eew_port()
        ws_servers_status = {}
        for port in (eew_port,):
            try:
                client_count = len(self.clients_5000)
                cache_data = self.cache_mgr.get_fused_cache(port)
                cache_count = len(cache_data) if cache_data else 0

                ws_servers_status[f"端口{port}"] = {
                    "status": "运行中",
                    "客户端数": client_count,
                    "缓存数": cache_count,
                    "result": "通过"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["passed"] += 1
            except Exception as e:
                ws_servers_status[f"端口{port}"] = {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["failed"] += 1

        check_result["modules"]["WebSocket服务器"] = ws_servers_status

        # 3. 检查控制台 IPC（无 TCP 管理端口）
        try:
            import os

            ipc_on = os.environ.get("FUSED_CONSOLE_IPC", "").strip() == "1"
            check_result["modules"]["控制台IPC"] = {
                "stdin_ipc": {
                    "status": "已启用" if ipc_on else "未启用（独立运行模式）",
                    "result": "通过" if ipc_on else "警告",
                }
            }
            check_result["summary"]["total"] += 1
            if ipc_on:
                check_result["summary"]["passed"] += 1
            else:
                check_result["summary"]["warnings"] += 1
        except Exception as e:
            check_result["modules"]["控制台IPC"] = {
                "stdin_ipc": {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败",
                }
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 4. 检查Fan Studio连接状态
        if self.ws_client_mgr:
            try:
                connection_quality = self.ws_client_mgr.connection_health['connection_quality']
                quality_map = {
                    'good': '良好',
                    'fair': '一般',
                    'poor': '较差',
                    'unknown': '未知'
                }
                fanstudio_status = {
                    "当前服务器": self.ws_client_mgr.current_server_url,
                    "主服务器健康度": round(self.ws_client_mgr.connection_health['primary_server_health'], 2),
                    "备用服务器健康度": round(self.ws_client_mgr.connection_health['backup_server_health'], 2),
                    "连接质量": quality_map.get(connection_quality, connection_quality),
                    "连续失败次数": self.ws_client_mgr.connection_stats['current_fail_streak'],
                    "result": "通过" if connection_quality != 'poor' else "警告"
                }
                check_result["modules"]["Fan Studio连接"] = fanstudio_status
                check_result["summary"]["total"] += 1
                if fanstudio_status["result"] == "通过":
                    check_result["summary"]["passed"] += 1
                else:
                    check_result["summary"]["warnings"] += 1
            except Exception as e:
                check_result["modules"]["Fan Studio连接"] = {
                    "status": "检查失败",
                    "error": str(e),
                    "result": "失败"
                }
                check_result["summary"]["total"] += 1
                check_result["summary"]["failed"] += 1

        # 5. 检查缓存管理器
        try:
            cache_status = {
                "内存缓存数量": len(self.cache_mgr.memory_cache),
                "融合缓存5000": len(self.cache_mgr.fused_cache_5000),
                "结果": "通过"
            }
            check_result["modules"]["缓存管理器"] = cache_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["缓存管理器"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 6. 检查翻译服务
        try:
            translation_cache_file = os.path.join(self.config.TRANSLATION_CACHE_DIR, "translation_cache_eew.json")
            cache_file_exists = os.path.exists(translation_cache_file)
            cache_count = 0
            if cache_file_exists:
                try:
                    with open(translation_cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                        cache_count = len(cache_data) if isinstance(cache_data, dict) else 0
                except Exception:
                    pass

            translation_status = {
                "缓存文件存在": cache_file_exists,
                "缓存数量": cache_count,
                "result": "通过"
            }
            check_result["modules"]["翻译服务"] = translation_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["翻译服务"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        # 7. 检查IP管理器
        try:
            ip_stats = self.client_ip_manager.get_connection_stats()
            ip_manager_status = {
                "总IP数": ip_stats.get('总IP数', 0),
                "活跃IP数": ip_stats.get('活跃IP数', 0),
                "总连接数": ip_stats.get('总连接数', 0),
                "黑名单数量": ip_stats.get('黑名单IP数', 0),
                "result": "通过"
            }
            check_result["modules"]["IP管理器"] = ip_manager_status
            check_result["summary"]["total"] += 1
            check_result["summary"]["passed"] += 1
        except Exception as e:
            check_result["modules"]["IP管理器"] = {
                "status": "检查失败",
                "error": str(e),
                "result": "失败"
            }
            check_result["summary"]["total"] += 1
            check_result["summary"]["failed"] += 1

        return check_result

    def _format_check_result_text(self, check_result: Dict[str, Any]) -> str:
        """格式化检查结果为文本"""
        response = "=== 自动检查结果 ===\n\n"
        response += f"检查时间: {check_result.get('timestamp', 'N/A')}\n\n"

        summary = check_result.get('summary', {})
        response += f"检查摘要: 总计 {summary.get('total', 0)} 项, "
        response += f"通过 {summary.get('passed', 0)} 项, "
        response += f"警告 {summary.get('warnings', 0)} 项, "
        response += f"失败 {summary.get('failed', 0)} 项\n\n"

        modules = check_result.get('modules', {})
        for module_name, module_data in modules.items():
            response += f"【{module_name}】\n"
            if isinstance(module_data, dict):
                for item_name, item_data in module_data.items():
                    if isinstance(item_data, dict):
                        # 获取状态和结果
                        status = item_data.get('status', '')
                        result = item_data.get('result', 'N/A')
                        result_symbol = "✓" if result == "通过" else "⚠" if result == "警告" else "✗"
                        
                        # 如果有status字段，显示状态行
                        if status:
                            response += f"  {result_symbol} {item_name}: {status}\n"
                        else:
                            # 没有status字段，直接显示名称和结果
                            response += f"  {result_symbol} {item_name}\n"
                        
                        # 自动显示所有其他字段（排除status、result、error）
                        excluded_keys = {'status', 'result', 'error'}
                        for key, value in item_data.items():
                            if key in excluded_keys:
                                continue
                            
                            # 格式化显示值
                            if isinstance(value, bool):
                                display_value = '是' if value else '否'
                            elif isinstance(value, (int, float)):
                                display_value = value
                            else:
                                display_value = value
                            
                            response += f"    {key}: {display_value}\n"
                        
                        # 如果有错误，最后显示
                        if 'error' in item_data:
                            response += f"    错误: {item_data['error']}\n"
                    else:
                        # 对于非字典值，直接显示键值对
                        if isinstance(item_data, bool):
                            display_value = '是' if item_data else '否'
                        else:
                            display_value = item_data
                        response += f"  {item_name}: {display_value}\n"
            response += "\n"

        return response.rstrip()

    def start_management_server(self, port: Optional[int] = None):
        """已弃用：管理改由控制台 stdin IPC，不再监听 TCP 端口。"""
        self.logger.debug("start_management_server 已弃用（控制台 IPC）")
