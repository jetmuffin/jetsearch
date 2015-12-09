import socket
import psutil


class Metric(object):
    @staticmethod
    def get_host():
        host = socket.getfqdn(socket.gethostname())
        return host

    @staticmethod
    def get_ip():
        host = socket.getfqdn(socket.gethostname())
        ip = socket.gethostbyname(host)
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

