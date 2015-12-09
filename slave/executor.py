# -*- coding: utf-8 -*-

import time
from kazoo.client import KazooClient
from metrics.metric import Metric
from mq.queue import FIFOQueue
from mq.filter import DuplicateFilter
from processor.processor import DocumentProcessor
from spiders.crawlers import SimpleCrawler
from storage.mongodb import MongodbStorage
from utils.encrypt import Encrypt
from pymongo import MongoClient
import redis
import logging

logger = logging.getLogger('root.SpiderWorker')
IN_TASK = False
task = {}


class Worker(object):
    def __init__(self, master='127.0.0.1:2181', type='spider'):

        self.type = type
        self.task = None

        # 连接master的zookeeper-server
        # 默认zk为standalone模式下的127.0.0.1:2181
        self.zk = KazooClient(hosts=master)
        self.zk.start()

        # 生成slave-id并向master注册
        self.id = Metric.get_host()
        self._register()

        # 获取分布式配置
        self.config = eval(self.zk.get("/jetsearch/config")[0])

        # 连接消息队列redis
        redis_host, redis_port = self.config.get("redis").split(":")
        self.redis = redis.Redis(host=redis_host, port=redis_port)

        # 连接数据库mongodb
        mongo_host, mongo_port = self.config.get("mongodb").split(":")
        self.mongodb = MongoClient(host=mongo_host, port=int(mongo_port))

        logger.info("[SUCESS] slave init with id %s and type %s", self.id, self.type)

        # 监听任务发布
        @self.zk.DataWatch("/jetsearch/task")
        def task_watch(data, stat):
            if data:
                self.task = eval(data)
                logger.info("[TASK] receve task: %s", data)

    def run(self, task):
        raise NotImplementedError

    def listen(self):
        while True:
            if self.task:
                self.run(self.task)
            else:
                logger.info("[%s] Wait for task assignment..." % self.type.upper())
                time.sleep(3)

    def _register(self):
        """
        向master注册slave节点,存入slave状态信息
        """
        if self.zk.exists("/jetsearch/slaves/" + self.id):
            self.id = self._generate_id()

        slave = {
            "id": self.id,
            "type": self.type,
            "host": Metric.get_host(),
            "addr": Metric.get_ip(),
            "heartbeat": Metric.get_heartbeat()
        }
        self.zk.create("/jetsearch/slaves/" + self.id, str(slave))

    def _generate_id(self):
        """
        使用md5(host+ip+timestamp)[0:8]slave_id
        :return: slave_id
        """
        host = Metric.get_host()
        ip = Metric.get_ip()
        return host + "-" + Encrypt.md5(host + ip + str(time.time()))[0:8]

    def __del__(self):
        """
        析构函数,删除slave节点信息
        """
        self.zk.delete("/jetsearch/slaves/" + self.id)
        self.zk.stop()


class SpiderWorker(Worker):
    def run(self, task):
        # 注册任务队列
        spider_queue = FIFOQueue(self.redis, self.config.get("spider_queue"))
        processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))

        # 注册过滤器
        duplicate_filter = DuplicateFilter(self.redis, self.config.get("duplicate_set"))

        # 注册存储数据库
        storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

        # 注册爬虫
        crawler = SimpleCrawler(task['start_url'], task['allowed_domain'])

        if len(spider_queue) > 0:
            url = spider_queue.pop()
            crawler.fetch(url)

            # 若爬虫成功爬取
            if crawler.success:
                item = crawler.parse()
                new_urls = item.get('links')

                # 抓去的新链接判重后加入队列
                for new_url in new_urls:
                    if not duplicate_filter.exists(new_url):
                        spider_queue.push(new_url)

                # url原始解析结果持久化
                item = storage_pipline.insert(item, self.config.get("page_table"))
                processer_queue.push(item.get('_id'))

                logger.info("[SUCCESS] %s." % url)
            else:
                logger.error("[FAILED] %s %s" % (url, crawler.error))

        else:
            logger.info("[SPIDER] Wait for some tasks...")
            time.sleep(3)


class ProcessorWorker(Worker):
    def run(self, task):
        # 注册任务队列
        processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))

        # 注册存储数据库
        storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

        # 注册文章处理器
        processor = DocumentProcessor()

        while len(processer_queue) > 0:
            doc_id = processer_queue.pop()
            document = storage_pipline.find(self.config.get("page_table"), doc_id)
            terms = processor.process(document)
            terms_count = len(terms)

            # update item information to db
            document['terms'] = terms
            storage_pipline.update(self.config.get("page_table"), doc_id, document)

            logger.info("[SUCCESS] %s" % document['href'])

        else:
            logger.info("[PROCESSOR] Wait for some tasks...")
            time.sleep(3)

