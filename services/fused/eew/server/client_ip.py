from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from services.fused.eew.config import Config

class ClientIPManager:
    """客户端IP管理器"""

    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # IP连接统计（仅保存当前仍在线的连接）
        self.ip_connections: Dict[str, Dict] = {}  # ip -> {connections: int, first_seen: float, last_seen: float, ports: Set[int]}

        # 内存中的历史连接记录（仅保留每个IP的最新一条，供“历史记录”命令快速查询）
        self.connection_history: List[Dict[str, Any]] = []

        # 历史连接记录持久化文件（完整历史）
        self.history_file = os.path.join(self.config.CACHE_DIR, "connection_history.jsonl")

        # 黑名单
        self.blacklist: Dict[str, float] = {}  # ip -> 过期时间戳，0表示永久封禁

        # 配置
        self.max_connections_per_ip = 20  # 每个IP最大连接数
        self.connection_timeout = 1800  # 连接超时时间（秒）- 30分钟

        # 文件路径
        self.blacklist_file = os.path.join(self.config.CACHE_DIR, "blacklist.json")

        # 锁
        self.lock = threading.RLock()

        # 清理过期连接的定时器
        self.cleanup_timer = None
        self.start_cleanup_timer()

        # 加载IP配置
        self.load_ip_config()

    def load_ip_config(self):
        """从文件加载IP配置"""
        try:
            # 加载黑名单
            if os.path.exists(self.blacklist_file):
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    blacklist_data = json.load(f)
                with self.lock:
                    self.blacklist = blacklist_data
                self.logger.info(f"已从 {self.blacklist_file} 加载黑名单: {len(self.blacklist)} 个IP")
            else:
                self.logger.info(f"黑名单配置文件 {self.blacklist_file} 不存在，使用空黑名单")


        except Exception as e:
            self.logger.error(f"加载IP配置文件失败: {e}")

        # 历史记录文件不在启动时整体加载，只在需要时按需读取，避免内存占用过大

    def append_history_file(self, history_entry: Dict[str, Any]):
        """将单条历史记录追加写入到独立文件（JSON Lines），用于完整追溯

        注意：只在连接完全断开时写入一次，长期累积形成完整历史。
        """
        try:
            os.makedirs(self.config.CACHE_DIR, exist_ok=True)
            with open(self.history_file, 'a', encoding='utf-8') as f:
                json.dump(history_entry, f, ensure_ascii=False)
                f.write("\n")
        except Exception as e:
            self.logger.error(f"追加写入历史连接文件失败: {e}")

    def load_full_history(self, ip: str = None) -> List[Dict[str, Any]]:
        """从文件读取完整历史记录（可按IP过滤）

        返回值：列表中按文件顺序（时间顺序）排列的原始记录。
        """
        records: List[Dict[str, Any]] = []
        try:
            if not os.path.exists(self.history_file):
                return records
            with open(self.history_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if ip is None or obj.get("IP地址") == ip:
                            records.append(obj)
                    except Exception:
                        # 单条解析失败不影响整体
                        continue
        except Exception as e:
            self.logger.error(f"读取历史连接文件失败: {e}")
        return records

    def save_blacklist(self):
        """保存黑名单到独立文件"""
        try:
            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump(self.blacklist, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"黑名单已保存到 {self.blacklist_file}")
        except Exception as e:
            self.logger.error(f"保存黑名单文件失败: {e}")

    def save_ip_config(self):
        """保存IP配置到文件（兼容旧接口）"""
        self.save_blacklist()

    def start_cleanup_timer(self):
        """启动定时任务（当前仅用于保留结构，不再自动清理任何记录）"""
        def cleanup_expired():
            # 按当前需求：不对连接记录和黑名单做任何自动清理，
            # 所有变更仅通过显式管理命令完成
            with self.lock:
                pass

            # 重新调度
            self.cleanup_timer = threading.Timer(300, cleanup_expired)  # 5分钟清理一次
            self.cleanup_timer.daemon = True
            self.cleanup_timer.start()

        cleanup_expired()

    def check_ip_allowed(self, client_ip: str) -> bool:
        """检查IP是否被允许连接"""
        with self.lock:
            # 检查黑名单（包含过期检查）
            if client_ip in self.blacklist:
                expiry = self.blacklist[client_ip]
                if expiry == 0 or time.time() < expiry:  # 永久封禁或未过期
                    return False
                else:  # 已过期，自动移除
                    del self.blacklist[client_ip]
                    self.logger.info(f"IP {client_ip} 黑名单封禁已过期，自动移除")
                    self.save_blacklist()

            return True

    def record_connection(self, client_ip: str, port: int):
        """记录客户端连接"""
        current_time = time.time()

        with self.lock:
            if client_ip not in self.ip_connections:
                self.ip_connections[client_ip] = {
                    'connections': 0,
                    'first_seen': current_time,
                    'last_seen': current_time,
                    'ports': set()
                }

            info = self.ip_connections[client_ip]
            info['connections'] += 1
            info['last_seen'] = current_time
            info['ports'].add(port)

    def record_disconnection(self, client_ip: str, port: int):
        """记录客户端断开"""
        with self.lock:
            if client_ip in self.ip_connections:
                info = self.ip_connections[client_ip]
                # 在修改前先快照端口信息，便于写入历史记录
                ports_snapshot = set(info.get('ports', set()))

                info['connections'] = max(0, info['connections'] - 1)
                if port in info['ports']:
                    info['ports'].discard(port)

                # 更新最后活动时间为断开时间
                disconnect_time = time.time()
                info['last_seen'] = disconnect_time

                # 如果该IP已无任何连接，则将其移入历史记录并从当前连接表中移除
                if info['connections'] == 0:
                    history_entry = {
                        "IP地址": client_ip,
                        "首次连接时间": info.get("first_seen", disconnect_time),
                        "最后活动时间": info.get("last_seen", disconnect_time),
                        "连接端口": sorted(list(ports_snapshot)) if ports_snapshot else [],
                        "断开时间": disconnect_time,
                    }
                    # 1) 内存中只保留该IP最新一条历史，用于“历史记录”命令
                    # 先移除旧记录，再追加新记录，确保每个IP只有一条最新记录
                    self.connection_history = [
                        h for h in self.connection_history if h.get("IP地址") != client_ip
                    ]
                    self.connection_history.append(history_entry)

                    # 2) 追加写入到历史文件，形成完整追溯
                    self.append_history_file(history_entry)
                    # 从当前连接表中移除该IP
                    del self.ip_connections[client_ip]
                    self.logger.debug(f"IP {client_ip} 已断开，移动到历史记录")

    def get_connection_history(self, ip: str = None) -> List[Dict[str, Any]]:
        """获取历史连接记录（每个IP仅保留最新一条，用于快速查看）

        Args:
            ip: 可选，指定IP时仅返回该IP的历史记录
        """
        with self.lock:
            if ip is None:
                return list(self.connection_history)
            return [h for h in self.connection_history if h.get("IP地址") == ip]

    def check_connection_limit(self, client_ip: str) -> bool:
        """检查连接数限制"""
        with self.lock:
            if client_ip in self.ip_connections:
                return self.ip_connections[client_ip]['connections'] < self.max_connections_per_ip
            return True

    @staticmethod
    def parse_duration(duration_str: str) -> int:
        """解析时间字符串为秒数
        
        Args:
            duration_str: 时间字符串，支持格式：30S, 5m, 2h, 1Y
                         支持单位：S(秒), m(分钟), h(小时), Y(年)
        
        Returns:
            秒数，如果解析失败或超出范围则返回None
        
        Raises:
            ValueError: 如果时间字符串格式不正确或超出范围
        """
        if not duration_str or not isinstance(duration_str, str):
            return None
        
        duration_str = duration_str.strip().upper()
        
        # 匹配数字和单位（支持大小写，已转换为大写）
        match = re.match(r'^(\d+)([SMHY])$', duration_str)
        if not match:
            raise ValueError(f"时间格式错误，支持格式：30S, 5m, 2h, 1Y（单位支持大小写）")
        
        value = int(match.group(1))
        unit = match.group(2)
        
        # 转换为秒数
        if unit == 'S':
            seconds = value
        elif unit == 'M':
            seconds = value * 60
        elif unit == 'H':
            seconds = value * 3600
        elif unit == 'Y':
            seconds = value * 365 * 24 * 3600
        else:
            raise ValueError(f"不支持的时间单位: {unit}，支持单位：S/s(秒), m/M(分钟), h/H(小时), Y/y(年)")
        
        # 验证范围：最低30秒，最高1年
        min_seconds = 30
        max_seconds = 365 * 24 * 3600  # 1年
        
        if seconds < min_seconds:
            raise ValueError(f"封禁时长不能低于30秒，当前值: {duration_str}")
        if seconds > max_seconds:
            raise ValueError(f"封禁时长不能超过1年，当前值: {duration_str}")
        
        return seconds

    def add_to_blacklist(self, ip: str, duration: Any = 0):
        """添加到黑名单

        Args:
            ip: IP地址
            duration: 封禁时长，支持以下格式：
                     - 0 或 None: 永久封禁
                     - 整数（秒数）: 直接指定秒数
                     - 字符串: 时间字符串，如 "30S", "5m", "2h", "1Y"
                              支持单位：S(秒), m(分钟), h(小时), Y(年)
                              限制：最低30秒，最高1年
        """
        with self.lock:
            duration_seconds = None
            
            # 处理永久封禁
            if duration == 0 or duration is None:
                self.blacklist[ip] = 0  # 永久封禁
                duration_str = "永久"
            else:
                # 尝试解析时间字符串
                if isinstance(duration, str):
                    try:
                        duration_seconds = self.parse_duration(duration)
                    except ValueError as e:
                        raise ValueError(f"时间解析失败: {e}")
                elif isinstance(duration, (int, float)):
                    # 兼容旧接口：如果是整数，假设是分钟数（向后兼容）
                    # 但如果值很大（>10000），可能是秒数
                    if duration > 10000:
                        duration_seconds = int(duration)
                    else:
                        duration_seconds = int(duration) * 60
                else:
                    raise ValueError(f"不支持的时间格式: {type(duration)}")
                
                # 验证范围
                if duration_seconds is not None:
                    min_seconds = 30
                    max_seconds = 365 * 24 * 3600  # 1年
                    
                    if duration_seconds < min_seconds:
                        raise ValueError(f"封禁时长不能低于30秒")
                    if duration_seconds > max_seconds:
                        raise ValueError(f"封禁时长不能超过1年")
                    
                    expiry = time.time() + duration_seconds
                    self.blacklist[ip] = expiry
                    
                    # 格式化显示时长
                    if duration_seconds < 60:
                        duration_str = f"{duration_seconds}秒"
                    elif duration_seconds < 3600:
                        duration_str = f"{duration_seconds // 60}分钟"
                    elif duration_seconds < 86400:
                        duration_str = f"{duration_seconds // 3600}小时"
                    elif duration_seconds < 365 * 24 * 3600:
                        duration_str = f"{duration_seconds // 86400}天"
                    else:
                        duration_str = f"{duration_seconds // (365 * 24 * 3600)}年"

            self.logger.info(f"IP {ip} 已添加到黑名单，封禁时长: {duration_str}")
            self.save_blacklist()

    def remove_from_blacklist(self, ip: str):
        """从黑名单移除"""
        with self.lock:
            if ip in self.blacklist:
                del self.blacklist[ip]
                self.logger.info(f"IP {ip} 已从黑名单移除")
                self.save_blacklist()

    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        with self.lock:
            total_connections = sum(info['connections'] for info in self.ip_connections.values())
            active_ips = len([ip for ip, info in self.ip_connections.items() if info['connections'] > 0])

            return {
                '总IP数': len(self.ip_connections),
                '活跃IP数': active_ips,
                '总连接数': total_connections,
                '黑名单IP数': len(self.blacklist),
                '每IP最大连接数': self.max_connections_per_ip
            }

    def get_ip_details(self, ip: str = None) -> Dict[str, Any]:
        """获取IP详情"""
        with self.lock:
            if ip:
                if ip in self.ip_connections:
                    return self.ip_connections[ip].copy()
                else:
                    return {}
            else:
                return self.ip_connections.copy()


# ============================================================================
# WebSocket服务器管理器
# ============================================================================

