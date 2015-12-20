# -*- coding: utf-8 -*-
from search.searcher import Searcher
while True:
    word = raw_input("输入搜索内容:")
    searcher = Searcher("127.0.0.1:27017", "jetsearch")
    searcher.search(word)