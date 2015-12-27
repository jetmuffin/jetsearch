# -*- coding: utf-8 -*-
import ConfigParser
import logging

import time
from kazoo.client import KazooClient
from pymongo import MongoClient

from processor.chain_processor import CompressProcessor, PagerankProcessor, ReverseProcessor, RankingProcessor

logger = logging.getLogger('root.Scheduler')


class Scheduler(object):
    def __init__(self, config_path="jetsearch.conf"):
        """
        任务调度器
        :param zk: zookeeper地址
        :param redis: redis地址
        :param mongodb: mongodb地址
        :return:
        """
        self.config = self._read_config(config_path)

        # 初始注册slave为空
        self.slaves = {}

        # 启动zookeeper
        # 保证节点存在
        # slaves存储子节点信息, job_done存储完成任务节点
        self.zk = KazooClient(hosts=zk)
        self.zk.start()
        self._init_zk()

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
                # 排除初始连接干扰的问题
                if len(disconnected_slave) != 0:
                    slave_id = disconnected_slave[0]
                    slave = self.slaves.pop(slave_id)

                    logger.info("[%s] %s lost connecttion." % (slave['type'].upper(), slave['id']))

        # 监听任务完成
        @self.zk.ChildrenWatch("/jetsearch/job_done")
        def job_watch(job_done):
            if len(job_done) == len(self.slaves) and len(job_done) > 0:
                job = self.zk.get("/jetsearch/job")[0]
                logger.info("[JOB] job finished %s " % job)
                self.zk.delete("/jetsearch/job")
                # self.chain_process()

    def update_config(self, config):
        """
        通过zookeeper更新任务配置
        :param config: 任务配置
        :return:
        """
        if self.zk.exists("/jetsearch/config"):
            self.zk.set("/jetsearch/config", str(config))
        else:
            self.zk.create("/jetsearch/config", str(config))

    def chain_process(self):
        """
        链式处理任务
        :return:
        """
        mongo_host, mongo_port = self.config.get("mongodb").split(":")
        mongo = MongoClient(mongo_host, int(mongo_port))
        db = mongo[self.config.get("storage_db")]

        chain = [
            CompressProcessor(db),
            PagerankProcessor(db),
            ReverseProcessor(db),
            RankingProcessor(db)
        ]

        start_time = time.time()
        # 对链式处理器进行处理
        for processor in chain:
            processor.fire()

        end_time = time.time()
        logger.info("Chain processor complete, took %f s" % (end_time - start_time))

    def _read_config(self, config_path):
        parser = ConfigParser.ConfigParser()
        config = {}

        parser.read(config_path)
        sections = parser.sections()
        for section in sections:
            options = parser.options(section)
            values = parser.items(section)
            for item in values:
                config[item[0]] = item[1]

        return config
        # config = {
        #     "zk": cf.get("zookeeper", "zookeeper_url"),
        #     "redis": cf.get("redis", "redis_url"),
        #     "mongodb": cf.get("mongodb", "mongodb_url"),
        #     "spider_queue": cf.get("redis", "spider_queue"),
        #     "processor_queue": cf.get("redis", "processor_queue"),
        #     "duplicate_set": cf.get("redis", "duplicate_set"),
        #     "storage_db": cf.get(""),
        #     "page_table": "tbl_page",
        #     "doc_table": "tbl_doc",
        #     "term_table": "tbl_term"
        # }

    def _init_zk(self):
        self.zk.ensure_path("/jetsearch")
        self.zk.delete("/jetsearch/slaves")
        self.zk.delete("/jetsearch/job_done")
        self.zk.ensure_path("/jetsearch/slaves")
        self.zk.ensure_path("/jetsearch/job_done")
        self.update_config(self.config)

    def __del__(self):
        self.zk.stop()
