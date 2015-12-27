# -*- coding: utf-8 -*-
from segmenters import JiebaSegmenter
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


class DocumentProcessor(object):
    def __init__(self):
        """
        页面处理器
        :return:
        """
        # 读取停词表
        stopwords_file = open("processor/stopwords")
        stopwords = []
        for line in stopwords_file.readlines():
            stopwords.append(line.strip())
        self.stopwords = set(stopwords)

    def process(self, document):
        """
        处理方法
        :param document: 页面信息
        :return:
        """

        # 加载分词器
        # 获取分词结果list
        segmenter = JiebaSegmenter()
        content_word_list = segmenter.segment(document['content'])
        title_word_list = []
        if document['title']:
            title_word_list = segmenter.segment(document['title'])

        # 统计词袋信息
        # "freq":         词频,
        # "title_freq":   标题词频,
        # "tf":           tf值,
        # "title_tf":     标题tf
        # "pos":          词出现位置,
        # "in_title":     是否出现在title中,
        terms = {}
        term_count = title_term_count = position = 0

        # 扫描content,统计frequent, position
        for word in content_word_list:
            position += 1
            term_count += 1
            # 过滤停词
            if self.filter(word):
                continue

            if word not in terms:
                terms[word] = {
                    "word": word,
                    "freq": 1,
                    "pos": [position],
                    "title_freq": 0
                }
            else:
                terms[word]['word'] = word
                terms[word]['freq'] += 1
                terms[word]['pos'].append(position)

        # 扫描title,统计title_freq
        for word in title_word_list:
            title_term_count += 1
            term_count += 1
            if word not in terms:
                terms[word] = {
                    "word": word,
                    "freq": 1,
                    "title_freq": 1,
                    "pos": []
                }
            else:
                if not terms[word].has_key('title_freq'):
                    terms[word]['title_freq'] = 1
                    terms[word]['freq'] += 1
                else:
                    terms[word]['freq'] += 1
                    terms[word]['title_freq'] += 1

        # 计算tf值, 删除freq键
        # tf = term_frequent / term_count
        for word in terms.keys():
            terms[word]['tf'] = float(terms[word]['freq']) / float(term_count)
            del terms[word]['freq']

        # 计算title_tf值,删除title_freq键
        for word in terms.keys():
            if title_term_count > 0:
                terms[word]['title_tf'] = float(terms[word]['title_freq']) / float(title_term_count)
            else:
                terms[word]['title_tf'] = 0
            del terms[word]['title_freq']

        # dict转为list
        terms_list = [terms[word] for word in terms.keys()]
        return terms_list

    def filter(self, word):
        """
        过滤方法,过滤指定规则匹配的词
        :param word: 需要判断是否过滤的词
        :return:
        """
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

document = {
    "title": "河海大学计算机与信息学院",
    "content": "河海大学是一所有近百年办学历史，以水利为特色，工科为主，多学科协调发展的教育部直属全国重点大学，是国家首批授权授予学"
}
