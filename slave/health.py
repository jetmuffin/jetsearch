# -*- coding: utf-8 -*-
import threading
import time
from metrics.metric import Metric


class Health(threading.Thread):
    def __init__(self, zk, slave_id):
        """
        健康监测线程
        :param zk: zookeeper地址
        :param slave_id: slave的id
        :return:
        """
        threading.Thread.__init__(self)
        self.end = False
        self.status = None
        self.zk = zk
        self.slave_id = slave_id

    def update(self, status):
        self.status = status

    def run(self):
        """
        未收到终止信号前,循环向zookeeper节点写入节点状态
        :return:
        """
        while not self.end:
            slave = eval(str(self.zk.get("/jetsearch/slaves/" + self.slave_id)[0]))
            slave['heartbeat'] = Metric.get_heartbeat()
            slave['task_status'] = self.status
            self.zk.set("/jetsearch/slaves/" + self.slave_id, str(slave))
            time.sleep(1)

    def stop(self):
        """
        终止线程
        :return:
        """
        self.end = True

