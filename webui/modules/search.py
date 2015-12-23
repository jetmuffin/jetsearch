from pprint import pprint

import time
import tornado

from master.searcher import Searcher


class SearchHandler(tornado.web.RequestHandler):
    def get(self):
        self.render(
            'search.html'
        )


class ResultHandler(tornado.web.RequestHandler):
    def get(self):
        keyword = self.get_argument('keyword')
        try:
            page = int(self.get_argument('page'))
        except:
            page = 1
        if not keyword:
            self.redirect('/search')

        searcher = Searcher("127.0.0.1:27017", "jetsearch")
        start = time.time()
        results = searcher.search(keyword, page)
        end = time.time()
        self.render(
            'result.html',
            results=results,
            time=(end-start),
            page=page,
            keyword=keyword
        )
