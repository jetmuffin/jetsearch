# -*- coding: utf-8 -*-
import json
from pprint import pprint

import pymongo
from bson import ObjectId
from pymongo import MongoClient

from mq.queue import FIFOQueue
from utils.encrypt import Encrypt
import sys
import redis
reload(sys)
sys.setdefaultencoding("utf-8")
server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch02

redis_server = redis.Redis("127.0.0.1", 6379)
queue = FIFOQueue(redis_server, "task:reprocessor")
for page in db.tbl_page.find():
    queue.push(page['_id'])