from pprint import pprint

from pymongo import MongoClient
from processor.segmenters import JiebaSegmenter


class Searcher(object):
    def __init__(self, mongo_addr, db):
        mongo_host, mongo_port = mongo_addr.split(":")
        self.server = MongoClient(mongo_host, int(mongo_port))
        self.db = self.server[db]
        self.segmenter = JiebaSegmenter()

    def search(self, word, page, page_size=10):
        word_list = self.analyze(word)
        docs = {}
        merge_docs = set()
        first = True
        for word in word_list:
            term = self.db.tbl_ranked_term.find_one({'_id': word})
            doc_list = set()
            if term:
                for term_doc in term['value']['docs']:
                    docs[term_doc['id']] = term_doc
                    doc_list.add(term_doc['id'])
                if first:
                    merge_docs = doc_list
                    first = False
                else:
                    merge_docs = merge_docs.intersection(doc_list)

        merge_docs = [docs[doc] for doc in merge_docs]
        merge_docs.sort(key=lambda x: x['rating'], reverse=True)

        page_count = len(merge_docs)/page_size
        page = min(page, page_count)
        page = max(page, 1)

        docs = []
        for i in range(10*page-9, 10*page):
            doc = self.db.tbl_page.find_one({"_id": merge_docs[i]["id"]})


            result_doc = {
                "rating": merge_docs[i]['rating'],
                "content": self.highlight(doc['content'], merge_docs[i]['pos']),
                "title": doc['title'],
                "href": doc['href'],
            }
            docs.append(result_doc)

        result = {
            "page_count": page_count,
            "docs": docs,
            "count": len(merge_docs)
        }
        return result

    def analyze(self, word):
        word_list = self.segmenter.segment(word, False)
        for word in word_list:
            print word
        return word_list

    def highlight(self, content, pos):
        word_list = self.segmenter.segment(content, False)
        range = 40
        print len(word_list)
        for position in pos:
            word_list[int(position)-1] = "<strong>%s</strong>" % word_list[int(position)-1]
        start = max(0, int(pos[0] - range))
        end = min(int(pos[0] + range), len(word_list))
        print start,end
        word_list = word_list[start:end]
        content = "".join(word_list)
        return content.strip()

    def highlight_title(self, title, words):
        for word in words:
            title.replace(word, "<strong>%s</strong>"%word)
        return title
