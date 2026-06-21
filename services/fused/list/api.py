from __future__ import annotations

from flask import jsonify

from services.fused.list.state import app, fused_data_lock, fused_events

class APIHandler:
    """API处理器：负责Flask应用的路由和API端点"""

    @staticmethod
    @app.route("/earthquakes")
    def earthquakes():
        """地震数据 API（端口 8150）"""
        with fused_data_lock:
            return jsonify({"shuju": list(fused_events)})

# ============================================================================
# 主程序模块
