from bson import Code
from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch

# print db.tbl_doc.find()

mapper = Code(open('pagerank_map.js', 'r').read())
reducer = Code(open('pagerank_reduce.js', 'r').read())

result = db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank", full_response=True)
print result


