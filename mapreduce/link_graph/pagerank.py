# -*- coding: utf-8 -*-
from pprint import pprint

from bson import Code, ObjectId
from pymongo import MongoClient
server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch02

judge_list = []
for doc in db.tbl_page.find().limit(100):
    judge_list.append({
        "_id": doc['_id'],
        "pr": doc['pr']
    })
convergence = 0

def conver_judge():
    global convergence
    for doc in judge_list:
        cur_doc = db.tbl_pagerank_t.find_one({"_id": doc['_id']})
        if cur_doc['value']['pr'] == doc['pr']:
            convergence += 1
            judge_list.remove(doc)
        else:
            doc['pr'] = cur_doc['value']['pr']

mapper = Code(open('pagerank_map.js', 'r').read())
reducer = Code(open('pagerank_reduce.js', 'r').read())

# 初始迭代次数为100
iteration = 100
count = 0

# 初次迭代,从pagerank表至pagerank_t表
db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank_t", full_response=True, verbos=True)

# pagerank_t表循环迭代
while count < iteration:
    db.tbl_pagerank_t.map_reduce(mapper, reducer, out="tbl_pagerank_t", full_response=True, verbos=True)
    count += 1
    conver_judge()
    print "%d pages convergence" % convergence
    if convergence == 100:
        break

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

