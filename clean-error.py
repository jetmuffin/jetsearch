# -*- coding: utf-8 -*-
import json
from pprint import pprint

from pymongo import MongoClient

from mq.queue import FIFOQueue
import redis

# redis = redis.Redis()
# spider_queue = FIFOQueue(redis, "task:spider")
# while len(spider_queue):
#     test = eval(spider_queue.pop())
#     if "apple.com" in test['url']:
#         print test['url']
#         continue
#     spider_queue.push(str(test))

server = MongoClient("127.0.0.1",27017)
db = server.jetsearch02
pprint(json.dumps(str(db.tbl_page.find_one({"href":"http://www.hhu.edu.cn"})), ensure_ascii=False))
