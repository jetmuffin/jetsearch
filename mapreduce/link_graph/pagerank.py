from bson import Code
from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch

mapper = Code(open('pagerank_map.js', 'r').read())
reducer = Code(open('pagerank_reduce.js', 'r').read())
# print db.tbl_doc.find()
iteration = 10
count = 0
id = db.tbl_pagerank.find_one()['_id']

print id
while count < iteration:
    db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank", full_response=True)
    pr_post = db.tbl_pagerank.find({'_id': id})[0]['value']['pr']
    print pr_post
    count += 1
    print count

