import time
from pymongo import MongoClient

from processor.segmenters import JiebaSegmenter


class Searcher(object):
    def __init__(self, mongo_addr, db):
        mongo_host, mongo_port = mongo_addr.split(":")
        self.server = MongoClient(mongo_host, int(mongo_port))
        self.db = self.server[db]

    def search(self, word):
        word_list = self.analyze(word)
        docs = {}
        result = set()
        first = True
        start = time.time()
        for word in word_list:
            term = self.db.tbl_term.find_one({'_id': word})
            doc_list = set()
            for term_doc in term['value']['docs']:
                docs[term_doc['id']] = term_doc
                doc_list.add(term_doc['id'])
            if first:
                result = doc_list
                first = False
            else:
                result = result.intersection(doc_list)

        result = [docs[doc] for doc in result]

        result.sort(key=lambda x:x['rating'],reverse=True)
        end = time.time()
        for i in range(len(result)):
            doc = self.db.tbl_page.find_one({"_id": result[i]["id"]})
            result[i]["title"] = doc['title']
            result[i]['href'] = doc['href']

        for doc in result:
            print ("%s : %s (%f)" % (doc['title'], doc['href'], doc['rating']))
        print "%f s" % (end-start)


    def analyze(self, word):
        segmenter = JiebaSegmenter()
        word_list = segmenter.segment(word, False)
        for word in word_list:
            print word
        return word_list
