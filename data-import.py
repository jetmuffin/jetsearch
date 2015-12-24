# -*- coding: utf-8 -*-
import json
from pprint import pprint

import pymongo
from bson import ObjectId
from pymongo import MongoClient

from utils.encrypt import Encrypt
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch02

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
# result = db.tbl_page.find_one({"href":"http://cies.hhu.edu.cn/s/36/t/65/main.htm"})
# result = db.tbl_page.find_one({"_id": ObjectId("567a5063e77a5418eb0a144b")}).update({
#         "$set": {
#             "pr": float(7.909579547587248e-06+2.5886511913862635e-06),
#         }
#     })
# db.tbl_page.remove({"_id": ObjectId("567a5367e77a54191ab7ef70")})
pprint(0.00003107978586131108+0.000024588981461647548+0.000012134302459623109)

