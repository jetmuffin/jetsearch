# -*- coding: utf-8 -*-
import logging
from kazoo.client import KazooClient

logger = logging.getLogger('root.Scheduler')

class Scheduler(object):
    def __init__(self, zk='127.0.0.1:2181', redis='127.0.0.1:6379', mongodb='127.0.0.1:27017'):
        self.config = {
            "zk": zk,
            "redis": redis,
            "mongodb": mongodb,
            "spider_queue": "task:spider",
            "processor_queue": "task:processor",
            "duplicate_set": "set:duplicate",
            "storage_db": "jetsearch",
            "page_table": "tbl_page",
            "doc_table": "tbl_doc",
            "term_table": "tbl_term"
        }

        # 初始注册slave为空
        self.slaves = {}

        # 启动zookeeper
        self.zk = KazooClient(hosts=zk)
        self.zk.start()

        # 保证节点存在
        self.zk.ensure_path("/jetsearch")
        self.zk.ensure_path("/jetsearch/slaves")

        self.update_config(self.config)

        # 监听slaves信息
        @self.zk.ChildrenWatch("/jetsearch/slaves")
        def slave_watch(slaves):
            # slave注册
            if len(slaves) > len(self.slaves.keys()):
                slave_id = [i for i in slaves if i not in self.slaves][0]
                slave = eval(self.zk.get("/jetsearch/slaves/" + slave_id)[0])
                self.slaves[slave_id] = slave

                logger.info("[%s] %s registered on %s" % (slave['type'].upper(), slave['id'], slave['addr']))

            # slave断开连接
            else:
                disconnected_slave = [i for i in self.slaves if i not in slaves]
                # 排除初始连接的问题
                if len(disconnected_slave) != 0:
                    slave_id = disconnected_slave[0]
                    slave = self.slaves.pop(slave_id)

                    logger.info("[%s] %s lost connecttion." % (slave['type'].upper(), slave['id']))

    def update_config(self, config):
        if self.zk.exists("/jetsearch/config"):
            self.zk.set("/jetsearch/config", str(config))
        else:
            self.zk.create("/jetsearch/config", str(config))

    def __del__(self):
        self.zk.stop()
