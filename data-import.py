# -*- coding: utf-8 -*-
import json
from pprint import pprint

import pymongo
from bson import ObjectId
from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch
# bulk = db.tbl_page.initialize_unordered_bulk_op()
#
# f = open("/Users/jeff/workspace/mongodb/page.json")
# count = 0
# while True:
#     line = f.readline()
#     count += 1
#     if not line:
#         break
#     if count % 100 == 0:
#         print count
#     obj = json.loads(line)
#     obj['_id'] = ObjectId(obj['_id']['$oid'])
#     bulk.insert(obj)
#
# result = bulk.execute()
# pprint(result)

# for result in db.tbl_page.find().sort("pr", pymongo.DESCENDING).limit(100):
#     pprint(result)
result = db.tbl_page.find_one({"href":"http://hhic.hhu.edu.cn/s/3/t/11/f5/40/info128320.htm"})
pprint(result)