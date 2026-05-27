"""List 子系统入口。"""

from __future__ import annotations

from services.fused.list.engine import MainHandler


class ListModule:
    def start(self) -> None:
        import signal
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        MainHandler.initialize()
        MainHandler.start_threads()
        MainHandler.start_servers()
        self._start_management()

    def _start_management(self) -> None:
        from services.list.management_ws import start_list_management_server
        import services.fused.list.engine as fl
        start_list_management_server(fl)
