from pprint import pprint

from pymongo import MongoClient
from bson import Code

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch02


mapper = Code(open('doc2term_map.js', 'r').read())
reducer = Code(open('doc2term_reduce.js', 'r').read())

result = db.tbl_page.map_reduce(mapper, reducer, out="tbl_term", full_response=True, verbose=True)
pprint(result)


