from bson import ObjectId
from pymongo import MongoClient
from pprint import pprint

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch
db.tbl_pagerank.drop()
bulk = db.tbl_pagerank.initialize_unordered_bulk_op()

pages_cursor = db.tbl_page.find()
convert_dic = {}
pages = []

for page in pages_cursor:
    convert_dic[page['href']] = page['_id']
    pages.append(page)

print len(pages)
prs = []
length = len(convert_dic)
for page in pages:
    page_links = page['links']
    compressed_links = []

    for link in page_links:
        if convert_dic.has_key(link):
            compressed_links.append(convert_dic[link])


    if(len(compressed_links)):
        pr = {
            "_id": page['_id'],
            "value": {
                "links": compressed_links,
                "pr":   float(1.0/length),
                "length": length
            }
        }
    else:
        pr = {
            "_id": page['_id'],
            "value": {
                "links": [],
                "pr":   float(1.0/length),
                "length": length
            }
        }
    prs.append(pr)

for pr in prs:
    pr['value']['length'] = length
    bulk.insert(pr)

result = bulk.execute()
pprint(result)

