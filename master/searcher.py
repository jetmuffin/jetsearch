# -*- coding: utf-8 -*-
from pprint import pprint

import time
from pymongo import MongoClient
from processor.segmenters import JiebaSegmenter


class Searcher(object):
    def __init__(self, mongo_addr, db):
        mongo_host, mongo_port = mongo_addr.split(":")
        self.server = MongoClient(mongo_host, int(mongo_port))
        self.db = self.server[db]
        self.segmenter = JiebaSegmenter()

    def search(self, keyword, page, page_size=10):
        keyword_list = self.analyze(keyword)
        docs = {}
        merge_docs = set()
        first = True

        for keyword in keyword_list:
            term = self.db.tbl_ranked_term.find_one({'_id': keyword})
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
                "title": self.highlight_title(doc['title'], keyword_list),
                "href": doc['href'],
            }
            docs.append(result_doc)

        result = {
            "page_count": page_count,
            "docs": docs,
            "count": len(merge_docs)
        }
        return result

    def search_new(self, keyword, page, page_size=10):
        keyword_list = self.analyze(keyword)
        results_dict = dict()
        start = time.time()
        for keyword in keyword_list:
            start2= time.time()
            term = self.db.tbl_ranked_term.find_one({'_id': keyword})
            end2 = time.time()
            print "1",end2-start2
            # 若为停词
            if not term:
                continue

            print len(term['value']['docs'])

            start3 = time.time()
            # 若该doc还未被加入结果dict
            for doc in term['value']['docs']:
                if not results_dict.get(doc['id']):
                    results_dict[doc['id']] = {}
                    results_dict[doc['id']]['rating'] = doc['rating']
                # 若该doc已经被加入结果dict,则在原结果上加上rating,扩充position
                else:
                    results_dict[doc['id']]['rating'] += doc['rating']
            end3 = time.time()
            print "2", end3-start3

        end = time.time()
        print end-start

        start4 = time.time()
        results_list = [(doc_id, results_dict[doc_id]['rating']) for doc_id in sorted(results_dict.keys())]
        results_list.sort(key=lambda x: x[1], reverse=True)
        end4 = time.time()
        print "4",end4-start4

        results_docs = []
        for i in range(10 * page - 9, 10 * page):
            doc = self.db.tbl_page.find_one({"_id": results_list[i][0]})
            results_docs.append({
                "rating": results_list[i][1],
                "content": doc['content'],
                "title": self.highlight_title(doc['title'], keyword_list),
                "href": doc['href'],
            })

        result = {
            "page_count": len(results_list)/page_size,
            "docs": results_docs,
            "count": len(results_list)
        }
        return result


    def analyze(self, word):
        word_list = self.segmenter.segment(word, False)
        for word in word_list:
            print word
        return word_list

    def highlight(self, content, pos):
        word_list = self.segmenter.segment(content, False)
        range = 100
        for position in pos:
            word_list[int(position)-1] = "<strong>%s</strong>" % word_list[int(position)-1]
        start = max(0, int(pos[0] - range))
        end = min(int(pos[0] + range), len(word_list))
        word_list = word_list[start:end]
        content = "".join(word_list)
        return content.strip()

    def highlight_title(self, title, words):
        """
        对标题进行高亮
        :param title: 标题
        :param words: 关键词列表
        :return: 高亮后的标题
        """
        for word in words:
            title = title.replace(word, "<strong>%s</strong>"%word)
        return title


