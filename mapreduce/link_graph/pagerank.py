# -*- coding: utf-8 -*-
from pprint import pprint

from bson import Code
from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch

mapper = Code(open('pagerank_map.js', 'r').read())
reducer = Code(open('pagerank_reduce.js', 'r').read())

# 初始迭代次数为10
iteration = 10
count = 0

page = db.tbl_pagerank.find_one()
pr_pre = page['value']['pr']

while count < iteration:
    db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank", full_response=True, verbos=True)
    pr_post = db.tbl_pagerank.find({'_id': page['_id']})[0]['value']['pr']
    print pr_post
    count += 1
    if pr_pre == pr_post:
        break
    pr_pre = pr_post

print "iteration: %d" % count

# 更新page表
bulk = db.tbl_page.initialize_ordered_bulk_op()
for page in db.tbl_pagerank.find():
    bulk.find({"_id": page["_id"]}).update({
        "$set": {
            "pr": float(page['value']['pr']),
        }
    })
result = bulk.execute()
pprint(result)
