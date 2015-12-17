from pymongo import MongoClient
from bson import Code

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch

# print db.tbl_doc.find()

mapper = Code(open('doc2term_map.js', 'r').read())
reducer = Code(open('doc2term_reduce.js', 'r').read())

result = db.tbl_doc.map_reduce(mapper, reducer, out="tbl_term", full_response=True)
print result


