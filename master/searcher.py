# -*- coding: utf-8 -*-
from pprint import pprint

import time

import math
from pymongo import MongoClient
from processor.segmenters import JiebaSegmenter
from bson import ObjectId


class Searcher(object):
    weights = {
        "tf": 0.05,
        "title_tf": 0.85,
        "pr": 0.1
    }

    def __init__(self, mongo_addr, db):
        mongo_host, mongo_port = mongo_addr.split(":")
        self.server = MongoClient(mongo_host, int(mongo_port))
        self.db = self.server[db]
        self.segmenter = JiebaSegmenter()

    def search(self, keyword, page, page_size=10):
        """
        搜索内容
        :param keyword: 查询词
        :param page: 页数
        :param page_size: 页框大小
        :return:
        """
        keyword = keyword.replace(' ', '')
        keyword_list = self.analyze(keyword)
        result_dict = {}
        for keyword in keyword_list:
            term = self.db.tbl_term.find_one({'_id': keyword})
            # 过滤停词
            if not term:
                continue
            # 处理该keyword的doc集合
            for doc in term['value']['docs']:
                rating = self.weights['tf'] * doc['tf'] + self.weights['title_tf'] * doc['title_tf'] + self.weights['pr']

                # 若doc未被加入result
                if not result_dict.has_key(doc['page_id']):
                    result_dict[doc['page_id']] = [rating]
                # 若已加入result
                else:
                    result_dict[doc['page_id']].append(rating)

        result_list = []
        for doc_id in result_dict.keys():
            # 过滤对某个keyword无相关性的文章
            if (len(result_dict[doc_id]) < len(keyword_list)):
                continue

            # 计算doc与query的相关度
            # 利用欧氏距离
            else:
                result_list.append({
                    "id": doc_id,
                    "rating": math.sqrt(sum([i * i for i in result_dict[doc_id]]))
                })

        result_list.sort(key=lambda x: x['rating'], reverse=True)

        print len(result_list)
        if len(result_list) == 0:
            result = {
                "page_count": 1,
                "docs": [],
                "count": 0
            }
        else:
            results_docs = []
            result_start = page_size*(page-1)+1
            result_end = min(len(result_list), page_size*page)
            for i in range(result_start, result_end):
                doc = self.db.tbl_page.find_one({"_id": result_list[i]['id']})
                results_docs.append({
                    "id": str(result_list[i]['id']),
                    "rating": result_list[i]['rating'],
                    "content": self.highlight(doc['content'], keyword_list),
                    "title": self.highlight_title(doc['title'], keyword_list),
                    "href": doc['href'],
                    "pr": doc['pr']
                })

            result = {
                "page_count": len(result_list) / page_size + 1,
                "docs": results_docs,
                "count": len(result_list)
            }
        return result

    def analyze(self, word):
        """
        对查询语句进行分析
        :param word: 查询语句
        :return:
        """
        # 读取停词表
        stopwords_file = open("processor/stopwords")
        stopwords = []
        for line in stopwords_file.readlines():
            stopwords.append(line.strip())
        word_list = self.segmenter.segment(word, False)

        for word in word_list:
            if word in stopwords or word == ' ':
                word_list.remove(word)
        return word_list

    def highlight(self, content, keywords):
        """
        对内容进行高亮
        :param content: 内容
        :param keywords: 关键词列表
        :return:
        """
        word_list = self.segmenter.segment(content, False)
        start_word_index = 0
        start_word_flag = True

        for i in xrange(len(word_list)):
            if word_list[i] in keywords:
                word_list[i] = word_list[i].replace(word_list[i], "<strong>%s</strong>" % word_list[i])
                # 记录第一个word位置
                if(start_word_flag):
                    start_word_index = i
                    start_word_flag = False

        range = 100
        start = max(0, int(start_word_index - range))
        end = min(int(start_word_index + range), len(word_list))
        word_list = word_list[start:end]
        content = "".join(word_list)
        return content.strip() + "..."

    def highlight_title(self, title, words):
        """
        对标题进行高亮
        :param title: 标题
        :param words: 关键词列表
        :return: 高亮后的标题
        """
        for word in words:
            title = title.replace(word, "<strong>%s</strong>" % word)
        return title
