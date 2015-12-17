# -*- coding: utf-8 -*-

import jieba
import jieba.analyse
import jieba.posseg


class Segmenter(object):
    def __init__(self):
        pass

    def segment(self, document):
        raise NotImplementedError


class JiebaSegmenter(Segmenter):
    def segment(self, content):
        """
        使用Jieba进行分词
        :param document: 文章爬取信息
        :return: list(words): 分词结果
        """
        # 允许多线程分词 - 4线程
        # jieba.enable_parallel(2)
        word_list = list(jieba.cut(content, False))

        return word_list

