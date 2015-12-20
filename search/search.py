# -*- coding: utf-8 -*-
from pymongo import ASCENDING, DESCENDING
from pymongo import MongoClient
import time

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch
db.tbl_term.ensure_index('_id', unique=True)
db.tbl_doc.ensure_index('page_id', unique=True)
db.tbl_page.ensure_index('href')

key = raw_input("输入搜索词:")
start = time.time()
term = db.tbl_term_mr.find_one({'_id': key})
docs = []

count = 0
for term_doc in term['value']['docs']:
#     # doc = db.tbl_doc.find_one({'page_id': term_doc['doc_id']})
    count += 1
    page = db.tbl_page.find_one({'_id': term_doc['id']})
    print "%s : %s , rating: %f" % (page['title'], page['href'], term_doc['rating'])
    if count > 10:
        break
    
end = time.time()
print "耗时: %f s" % float(end - start)
print "搜索到了: %d 个结果" % len(term['value']['docs'])
    # docs.append(doc)
# for doc in docs