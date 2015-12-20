# -*- coding: utf-8 -*-
from segmenters import JiebaSegmenter
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class DocumentProcessor(object):
    def __init__(self):

        # 读取停词表
        stopwords_file = open("processor/stopwords")
        stopwords = []
        for line in stopwords_file.readlines():
            stopwords.append(line.strip())
        self.stopwords = set(stopwords)

    def process(self, document):
        # 加载分词器
        # 获取分词结果list
        segmenter = JiebaSegmenter()
        word_list = segmenter.segment(document['content'])

        # 合并link中的contents
        link_contents = "#".join(document['link_contents'])

        # 统计词袋信息
        # "freq":         词频,
        # "tf":           tf值,
        # "pos":          词出现位置,
        # "in_title":     是否出现在title中,
        # "in_links":     是否出现在链接中
        terms = {}
        term_count = position = 0
        for word in word_list:
            position += 1
            # 过滤停词
            if self.filter(word):
                continue

            term_count += 1
            if document['title'] and word in document['title']:
                in_title = True
            else:
                in_title = False

            in_links = True if word in link_contents else False
            if word not in terms:
                terms[word] = {
                    "word": word,
                    "freq": 1,
                    "tf": 0,
                    "pos": [position],
                    "in_title": in_title,
                    "in_links": in_links
                }
            else:
                terms[word]['word'] = word
                terms[word]['freq'] += 1
                terms[word]['pos'].append(position)
                terms[word]['in_title'] = in_title
                terms[word]['in_links'] = in_links

        # 计算tf值, 删除freq键
        # tf = term_frequent / term_count
        for word in terms.keys():
            terms[word]['tf'] = float(terms[word]['freq']) / float(term_count)
            del terms[word]['freq']

        # dict转为list
        terms_list = [terms[word] for word in terms.keys()]
        return terms_list

    def filter(self, word):
        if not len(word):
            return True

        if word.encode("utf-8") in self.stopwords:
            return True

        if word == ' ':
            return True

        if '.' in word:
            return True

        if word.isdigit():
            return True

        return False



