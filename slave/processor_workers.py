# -*- coding: utf-8 -*-
import time

from mq.queue import FIFOQueue
from processor.document_processor import DocumentProcessor
from slave.worker import Worker
from storage.mongodb import MongodbStorage

def log(message):
    print message

class ProcessorWorker(Worker):
    def __init__(self, master='127.0.0.1:2181', type='spider'):
        """
        处理执行器
        :param master: 主节点地址
        :param type: 执行器类型
        :return:
        """
        Worker.__init__(self, master, type)

        # 注册任务队列
        self.processer_queue = FIFOQueue(self.redis, self.config.get("processor_queue"))
        # 注册存储数据库
        self.storage_pipline = MongodbStorage(self.mongodb, self.config.get("storage_db"))

    def run(self, job):
        # 注册文章处理器
        processor = DocumentProcessor()

        while len(self.processer_queue) > 0:
            self._update_on_job(True)
            page_id = self.processer_queue.pop()
            page = self.storage_pipline.find(self.config.get("page_table"), page_id)
            terms = processor.process(page)

            # 将page_table抽取的信息存入doc_table
            page['terms'] = terms
            self.storage_pipline.update(self.config.get("page_table"), page_id, page)

            self._update_status(True)
            log("[SUCCESS] %s" % page['href'])

        else:
            self.wait_task_time += 1
            if self.wait_task_time > 5:
                self._update_on_job(False)
            log("[PROCESSOR] Wait for some jobs...")
            time.sleep(3)
