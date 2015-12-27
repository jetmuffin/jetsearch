# -*- coding: utf-8 -*-
import redis
import time
from kazoo.client import KazooClient
from pymongo import MongoClient

from metrics.metric import Metric
from slave.health import Health
from utils.encrypt import Encrypt


def log(message):
    print message

class Worker(object):
    def __init__(self, master='127.0.0.1:2181', type='spider'):
        """
        子节点执行器
        :param master: 主节点地址
        :param type: 节点类别
        :return:
        """
        self.type = type
        self.job = None
        self.task = None
        self.job_status = {
            "total": 0,
            "success": 0,
            "fail": 0
        }
        self.wait_task_time = 0

        # 连接master的zookeeper-server
        # 默认zk为standalone模式下的127.0.0.1:2181
        self.zk = KazooClient(hosts=master)
        self.zk.start()

        # 生成slave-id并向master注册
        self.id = Metric.get_host()
        self._register()
        # 创建心跳线程
        self.health_check = Health(self.zk, self.id)
        self.health_check.update(self.job_status)

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
        def job_watch(data, stat, event):
            if data:
                self.job = eval(data)
                log("[JOB] receve job: %s" % data)
            else:
                if self.job:
                    self.job = None

    def run(self, job):
        raise NotImplementedError

    def task_stop(self):
        raise NotImplementedError

    def listen(self):
        """
        子节点等待任务发布
        :return:
        """
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
        """
        更新任务状态
        :param success: 任务是否成功
        :return:
        """
        self.job_status['total'] += 1
        if success:
            self.job_status['success'] += 1
        else:
            self.job_status['fail'] += 1
        self.health_check.update(self.job_status)

    def _update_on_job(self, working=True):
        """
        更新工作状态
        :param working: 节点是否在工作
        :return:
        """
        if working:
            if self.zk.exists("/jetsearch/job_done/" + self.id):
                self.zk.delete("/jetsearch/job_done/" + self.id)
        else:
            self.zk.ensure_path("/jetsearch/job_done/" + self.id)

    def disconect(self):
        """
        停止心跳,删除slave节点信息
        """
        log("[%s] disconnect from master...." % self.type)
        self.task_stop()
        self.health_check.stop()
        self.health_check.join()
        self.zk.delete("/jetsearch/slaves/" + self.id)
        self.zk.stop()
        log("[%s] bye ~ :)" % self.type.upper())

