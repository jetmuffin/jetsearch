# -*- coding: utf-8 -*-

import random
import time
from datetime import timedelta
from tornado import httpclient, gen, ioloop, queues
from mq.queue import FIFOQueue
from mq.filter import DuplicateFilter
from slave.worker import Worker
from spiders.crawlers import SimpleCrawler
from spiders.parser import NormalParser
from spiders.phantom_crawlers import PhantomCrawler
from storage.mongodb import MongodbStorage
import time

# logger = logging.getLogger('root.SpiderWorker')
IN_TASK = False
job = {}


def log(message):
    print message


class SpiderWorker(Worker):
    def __init__(self, master='127.0.0.1:2181', type='spider'):
        """
        spider类型执行器
        :param master: 主节点地址
        :param type: 执行器类型
        :return:
        """
        Worker.__init__(self, master, type)
        # 注册任务队列
        self.spider_queue = FIFOQueue(self.redis, self.config.get("spider_queue"))
        self.processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))
        # 注册过滤器
        self.duplicate_filter = DuplicateFilter(self.redis, self.config.get("duplicate_set"))
        # 注册存储数据库
        self.storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

    def run(self, job):
        """
        执行方法
        :param job: 任务信息
        :return:
        """
        # 注册爬虫
        crawler = PhantomCrawler()
        parser = NormalParser(job)

        if len(self.spider_queue) > 0:
            task = eval(self.spider_queue.pop())

            # 若该任务失败次数过多,不再处理该任务
            if task['life'] == 0:
                return

            response = crawler.fetch(task['url'])
            # success, result = crawler.fetch(task['url'])

            # 若爬虫成功爬取
            if response['status_code'] == 200:
                try:
                    item = parser.parse(task['url'], response['content'])
                    # 分片写入
                    item['ram'] = random.random()
                    new_urls = item['links']

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

                    # 更新任务状态
                    self._update_status(True)
                    log("[SUCCESS] %s." % task['url'])
                except Exception, e:
                    # 将失败的url再次放入队列
                    self.spider_queue.push({
                        "url": task['url'],
                        "life": task['life'] - 1
                    })
                    log("[FAILED] %s %s" % (task['url'], e))
            else:
                # 更新任务状态
                self._update_status(False)

                # 将失败的url再次放入队列
                self.spider_queue.push({
                    "url": task['url'],
                    "life": task['life'] - 1
                })
                log("[FAILED] %s %s" % (task['url'], response['status_code']))

        else:
            log("[SPIDER] Wait for some jobs...")
            time.sleep(3)


class AsyncSpiderWorker(Worker):
    def __init__(self, master='127.0.0.1:2181', type='spider', concurrency=5, **kwargs):
        """
        异步爬虫执行器
        :param master: 主节点地址
        :param type: 执行器类型
        :param concurrency: 并发数
        :param kwargs:
        :return:
        """
        Worker.__init__(self, master, type)
        # 注册任务队列
        self.spider_queue = FIFOQueue(self.redis, self.config.get("spider_queue"))
        self.processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))
        # 注册过滤器
        self.duplicate_filter = DuplicateFilter(self.redis, self.config.get("duplicate_set"))
        # 注册存储数据库
        self.storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))
        # 并发线程数
        self.concurrency = concurrency
        # 内部队列
        self._queue = queues.Queue()

    def fetch(self, url, **kwargs):
        fetch = getattr(httpclient.AsyncHTTPClient(), 'fetch')
        return fetch(url, raise_error=False, **kwargs)

    def parse(self, url, html):
        """ 解析html页面 """
        parser = NormalParser(url, html, self.job)
        item = parser.parse()
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
        self._update_status(True)
        log("[SUCCESS] %s." % url)

    def handle_response(self, task, response):
        """ 处理异步返回 """
        if response.code == 200:
            self.parse(task['url'], response.body)

        else:
            self._update_status(False)
            self.spider_queue.push({
                "url": task['url'],
                "life": task['life'] - 1
            })
            log("[FAILED] %s %s" % (task['url'], response.code))

    @gen.coroutine
    def get_page(self, task):
        """
        获取页面内容
        :param task:
        :return:
        """
        try:
            response = yield self.fetch(task['url'])
        except Exception as e:
            print('Exception: %s %s' % (e, task['url']))
            raise gen.Return(e)
        raise gen.Return(response)

    @gen.coroutine
    def _run(self):
        @gen.coroutine
        def fetch_url():
            current_task = yield self._queue.get()
            try:
                response = yield self.get_page(current_task)
                self.handle_response(current_task, response)

                # 从分布式队列中取出$(concurrency)个任务加入队列
                for i in range(self.concurrency):
                    if len(self.spider_queue) > 0:
                        task = eval(self.spider_queue.pop())
                        if task['life'] > 0:
                            yield self._queue.put(task)

            finally:
                self._queue.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        if len(self.spider_queue) > 0:
            self._update_on_job(True)
            # 加入首个任务
            self._queue.put(eval(self.spider_queue.pop()))

            # 启动worker直到队列为空
            for _ in range(self.concurrency):
                worker()

            yield self._queue.join(timeout=timedelta(seconds=300000))
        else:
            self.wait_task_time += 1
            if self.wait_task_time > 5:
                self._update_on_job(False)
            log("[SPIDER] Wait for some jobs...")
            time.sleep(3)

    def run(self, job):
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self._run)

