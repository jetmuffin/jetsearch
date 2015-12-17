from bson import ObjectId
from pymongo import MongoClient
from pprint import pprint

server = MongoClient("127.0.0.1", 27017)
db = server.jetsearch
bulk = db.tbl_pagerank.initialize_unordered_bulk_op()

pages_cursor = db.tbl_doc.find()
convert_dic = {}
pages = []

for page in pages_cursor:
    convert_dic[page['href']] = page['page_id']
    pages.append(page)

for page in pages:
    page_links = page['links']
    compressed_links = []

    for link in page_links:
        if convert_dic.has_key(link):
            compressed_links.append(convert_dic[link])

    if(len(compressed_links)):
        bulk.insert({
            "_id": page['page_id'],
            "value": {
                "links": compressed_links,
                "pr":   float(1.0/len(convert_dic))
            }
        })

result = bulk.execute()
pprint(result)

