# -*- coding: utf-8 -*-
import socket
import psutil


class Metric(object):
    @staticmethod
    def get_host():
        # TODO 修复BUG
        # host = socket.getfqdn(socket.gethostname())
        host = "localhost"
        return host

    @staticmethod
    def get_ip():
        # TODO 修复BUG
        # host = socket.getfqdn(socket.gethostname())
        # ip = socket.gethostbyname(host)
        ip = "127.0.0.1"
        return ip

    @staticmethod
    def get_heartbeat():
        phymem = psutil.virtual_memory()
        heartbeat = {
            "cpu": str(psutil.cpu_percent(1)) + "%",
            "mem_used": str(int(phymem.used / 1024 / 1024)) + "MB",
            "mem_total": str(int(phymem.total / 1024 / 1024)) + "MB",
            "mem_percent": str(phymem.percent) + "%"
        }
        return heartbeat

