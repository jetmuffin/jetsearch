# -*- coding: utf-8 -*-
from pprint import pprint

from bson import Code, ObjectId
from pymongo import MongoClient
server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch02

def judge(type=1):
    judge_list = [
        {"_id": ObjectId("567a4ff3e77a5418ef020006"), "pr": 0},
        {"_id": ObjectId("567a5c20e77a541afde34705"), "pr": 0},
        {"_id": ObjectId("567a5063e77a5418eb0a144b"), "pr": 0},
        {"_id": ObjectId("567a521be77a54191ab7ee54"), "pr": 0},
        {"_id": ObjectId("567a4ff6e77a5418ef020007"), "pr": 0}
    ]

    collection = db.tbl_pagerank if type == 1 else db.tbl_pagerank_t
    for item in judge_list:
        item['pr'] = collection.find_one({"_id": item['_id']})['value']['pr']
    return judge_list

mapper = Code(open('pagerank_map.js', 'r').read())
reducer = Code(open('pagerank_reduce.js', 'r').read())

# 初始迭代次数为30
iteration = 30
count = 0

# 初次迭代,从pagerank表至pagerank_t表
db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank_t", full_response=True, verbos=True)

# pagerank_t表循环迭代
while count < iteration:
    stop_list = judge(2)
    print(stop_list)
    db.tbl_pagerank_t.map_reduce(mapper, reducer, out="tbl_pagerank_t", full_response=True, verbos=True)
    count += 1
    stop_list_post = judge(2)
    if cmp(stop_list, stop_list_post) == 0:
        break
    stop_list = stop_list_post

print "iteration: %d" % count
# 更新page表
bulk = db.tbl_page.initialize_ordered_bulk_op()
for page in db.tbl_pagerank_t.find():
    bulk.find({"_id": page["_id"]}).update({
        "$set": {
            "pr": float(page['value']['pr']),
        }
    })
result = bulk.execute()
pprint(result)

