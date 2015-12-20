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
    def segment(self, content, cut_all=False):
        """
        使用Jieba进行分词
        :param document: 信息
        :return: list(words): 分词结果
        """
        # 允许多线程分词 - 4线程
        # jieba.enable_parallel(2)
        word_list = list(jieba.cut(content, cut_all))

        return word_list

