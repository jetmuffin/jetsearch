# -*- coding: utf-8 -*-

from bson import Code
from pymongo import MongoClient

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch


mapper = Code(open('ranking_map.js', 'r').read())
reducer = Code(open('ranking_reduce.js', 'r').read())
result = db.tbl_term.map_reduce(mapper, reducer, out="tbl_ranked_term", full_response=True)
print result