# -*- coding: utf-8 -*-
import random
import time
from kazoo.client import KazooClient
from metrics.metric import Metric
from mq.queue import FIFOQueue
from mq.filter import DuplicateFilter
from processor.processor import DocumentProcessor
from slave.health import Health
from spiders.crawlers import SimpleCrawler
from storage.mongodb import MongodbStorage
from utils.encrypt import Encrypt
from pymongo import MongoClient
import redis
import logging
import time

# logger = logging.getLogger('root.SpiderWorker')
IN_TASK = False
job = {}


def log(message):
    print message


class Worker(object):
    def __init__(self, master='127.0.0.1:2181', type='spider'):
        self.type = type
        self.job = None
        self.job_status = {
            "total": 0,
            "success": 0,
            "fail": 0
        }

        # 连接master的zookeeper-server
        # 默认zk为standalone模式下的127.0.0.1:2181
        self.zk = KazooClient(hosts=master)
        self.zk.start()

        # 生成slave-id并向master注册
        self.id = Metric.get_host()
        self._register()
        # 创建心跳线程
        self.health_check = Health(self.zk, self.id)

        # 获取分布式配置
        self.config = eval(self.zk.get("/jetsearch/config")[0])

        # 连接消息队列redis
        redis_host, redis_port = self.config.get("redis").split(":")
        self.redis = redis.Redis(host=redis_host, port=redis_port)

        # 连接数据库mongodb
        mongo_host, mongo_port = self.config.get("mongodb").split(":")
        self.mongodb = MongoClient(host=mongo_host, port=int(mongo_port))

        # 开始心跳
        self.health_check.start()
        log("[SUCCESS] slave init with id %s and type %s" % (self.id, self.type))

        # 监听任务发布
        @self.zk.DataWatch("/jetsearch/job")
        def job_watch(data, stat):
            if data:
                self.job = eval(data)
                log("[JOB] receve job: %s" % data)

    def run(self, job):
        raise NotImplementedError

    def listen(self):
        while True:
            if self.job:
                self.run(self.job)
            else:
                log("[%s] Wait for job assignment..." % self.type.upper())
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

    def _update_status(self, success=True):
        self.job_status['total'] += 1
        if success:
            self.job_status['success'] += 1
        else:
            self.job_status['fail'] += 1
        self.health_check.update(self.job_status)

    def disconect(self):
        """
        停止心跳,删除slave节点信息
        """
        log("[%s] disconnect from master...." % self.type)
        self.health_check.stop()
        self.health_check.join()
        self.zk.delete("/jetsearch/slaves/" + self.id)
        self.zk.stop()
        log("[%s] bye ~ :)" % self.type.upper())


class SpiderWorker(Worker):
    def __init__(self,  master='127.0.0.1:2181', type='spider'):
        Worker.__init__(self, master, type)

        # 注册任务队列
        self.spider_queue = FIFOQueue(self.redis, self.config.get("spider_queue"))
        self.processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))
        # 注册过滤器
        self.duplicate_filter = DuplicateFilter(self.redis, self.config.get("duplicate_set"))
        # 注册存储数据库
        self.storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

    def run(self, job):
        # 注册爬虫
        crawler = SimpleCrawler(job['start_url'], job['allowed_domain'])

        if len(self.spider_queue) > 0:
            task = eval(self.spider_queue.pop())

            # 若该任务失败次数过多,不再处理该任务
            if task['life'] == 0:
                return

            crawler.fetch(task['url'])
            # 若爬虫成功爬取
            if crawler.success:
                # 更新任务状态
                self._update_status(True)
                try:
                    item = crawler.parse()
                    # 分片写入
                    item['ram'] = random.random()
                    new_urls = item.get('links')

                    # 抓去的新链接判重后加入队列
                    for new_url in new_urls:
                        if not self.duplicate_filter.exists(new_url):
                            self.spider_queue.push({
                                "url": new_url,
                                "life": 5
                            })

                    # url原始解析结果持久化
                    item = self.storage_pipline.insert(self.config.get("page_table"), item)
                    self.processer_queue.push(item.get('_id'))

                    log("[SUCCESS] %s." % task['url'])
                except Exception, e:
                    # 将失败的url再次放入队列
                    self.spider_queue.push({
                        "url": task['url'],
                        "life": task['life']-1
                    })
                    log("[FAILED] %s %s" % (task['url'], e))
            else:
                # 更新任务状态
                self._update_status(False)

                # 将失败的url再次放入队列
                self.spider_queue.push({
                        "url": task['url'],
                        "life": task['life']-1
                    })
                log("[FAILED] %s %s" % (task['url'], crawler.error))

        else:
            log("[SPIDER] Wait for some jobs...")
            time.sleep(3)


class ProcessorWorker(Worker):
    def __init__(self, master='127.0.0.1:2181', type='spider'):
        Worker.__init__(self, master, type)

        # 注册任务队列
        self.processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))
        # 注册存储数据库
        self.storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

    def run(self, job):
        # 注册文章处理器
        processor = DocumentProcessor()

        while len(self.processer_queue) > 0:
            page_id = self.processer_queue.pop()
            page = self.storage_pipline.find(self.config.get("page_table"), page_id)
            terms = processor.process(page)

            # 将page_table抽取的信息存入doc_table
            page['terms'] = terms
            self.storage_pipline.update(self.config.get("page_table"), page_id, page)

            self._update_status(True)
            log("[SUCCESS] %s" % page['href'])

        else:
            log("[PROCESSOR] Wait for some jobs...")
            time.sleep(3)
