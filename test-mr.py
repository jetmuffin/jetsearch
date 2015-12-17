from pymongo import MongoClient
from bson import Code

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch

# print db.tbl_doc.find()

mapper = Code(open('map.js', 'r').read())
reducer = Code(open('reduce.js', 'r').read())

# print db.tbl_doc.find_one()
result = db.tbl_doc.map_reduce(mapper, reducer, out="results", full_response=True)
print result


