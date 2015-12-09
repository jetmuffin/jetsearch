# -*- coding: utf-8 -*-
from jetsearch.zk.zkmaster import ZKMaster

class Master(object):

    def __init__(self, zk='127.0.0.1:2181', redis='127.0.0.1:6379'):
        self.zk = ZKMaster(hosts=zk)
        self.redis = redis
