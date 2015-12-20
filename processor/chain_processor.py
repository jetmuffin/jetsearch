# -*- coding: utf-8 -*-
import logging
from pprint import pprint
from bson import Code
from pymongo import MongoClient

class Processor(object):
    def __init__(self, db):
        self.db = db

    def fire(self):
        raise NotImplementedError


class CompressProcessor(Processor):
    def fire(self):
        logging.info("[PROCESS] start compressing page information...")
        self.db.tbl_pagerank.drop()
        bulk = self.db.tbl_pagerank.initialize_unordered_bulk_op()

        pages_cursor = self.db.tbl_page.find()
        convert_dic = {}
        pages = []

        for page in pages_cursor:
            convert_dic[page['href']] = page['_id']
            pages.append(page)

        prs = []
        for page in pages:
            page_links = page['links']
            compressed_links = []

            for link in page_links:
                if convert_dic.has_key(link):
                    compressed_links.append(convert_dic[link])

            if (len(compressed_links)):
                pr = {
                    "_id": page['_id'],
                    "value": {
                        "links": compressed_links,
                        "pr": float(1.0 / len(convert_dic))
                    }
                }
                prs.append(pr)
        length = len(prs)
        for pr in prs:
            pr['value']['length'] = length
            bulk.insert(pr)

        result = bulk.execute()
        pprint(result)


class PagerankProcessor(Processor):
    def fire(self):
        logging.info("[PROCESS] start calculating pagerank of each page...")

        mapper = Code(open('mapreduce/link_graph/pagerank_map.js', 'r').read())
        reducer = Code(open('mapreduce/link_graph/pagerank_reduce.js', 'r').read())

        # 初始迭代次数为10
        iteration = 10
        count = 0

        page = self.db.tbl_pagerank.find_one()
        pr_pre = page['value']['pr']

        while count < iteration:
            self.db.tbl_pagerank.map_reduce(mapper, reducer, out="tbl_pagerank", full_response=True, verbos=True)
            pr_post = self.db.tbl_pagerank.find({'_id': page['_id']})[0]['value']['pr']
            print pr_post
            count += 1
            if pr_pre == pr_post:
                break
            pr_pre = pr_post

        print "iteration: %d" % count

        # 更新page表
        bulk = self.db.tbl_page.initialize_ordered_bulk_op()
        for page in self.db.tbl_pagerank.find():
            bulk.find({"_id": page["_id"]}).update({
                "$set": {
                    "pr": float(page['value']['pr']),
                }
            })
        result = bulk.execute()
        pprint(result)


class ReverseProcessor(Processor):
    def fire(self):
        logging.info("[PROCESS] start adding reverse index to each term...")

        mapper = Code(open('mapreduce/reverse_index/doc2term_map.js', 'r').read())
        reducer = Code(open('mapreduce/reverse_index/doc2term_reduce.js', 'r').read())

        result = self.db.tbl_page.map_reduce(mapper, reducer, out="tbl_term", full_response=True)
        pprint(result)

class RankingProcessor(Processor):
    def fire(self):
        logging.info("[PROCESS] start ranking each page of each term...")

        mapper = Code(open('mapreduce/ranking/ranking_map.js', 'r').read())
        reducer = Code(open('mapreduce/ranking/ranking_reduce.js', 'r').read())

        result = self.db.tbl_term.map_reduce(mapper, reducer, out="tbl_term", full_response=True)
        pprint(result)


class IndexProcessor(Processor):
    def fire(self):
        logging.info("[PROCESS] start adding index to each table...")

        self.db.tbl_term.ensure_index('_id', unique=True)
        self.db.tbl_page.ensure_index('href')