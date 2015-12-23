# -*- coding: utf-8 -*-
import time

from search.searcher import Searcher
while True:
    word = raw_input("输入搜索内容:")
    searcher = Searcher("127.0.0.1:27017", "jetsearch")
    start = time.time()
    result = searcher.search(word, 0, 10)
    end = time.time()
    for doc in result:
        print "%s %s %f" % (doc['title'], doc['href'], doc['rating'])
    print "spend %f s" % (end-start)
