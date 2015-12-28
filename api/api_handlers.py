import json

import tornado
import tornado.web
import tornado.escape
from kazoo.client import KazooClient

from master.searcher import Searcher
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

class QueryAPI(tornado.web.RequestHandler):
    def get(self):
        keyword = self.get_argument('keyword')
        try:
            page = int(self.get_argument('page'))
        except:
            page = 1
        searcher = Searcher("127.0.0.1:27017", "jetsearch02")
        results = json.dumps(searcher.search_two(keyword, page), ensure_ascii=False)
        results = tornado.escape.json_decode(results)
        self.write(results)


class SlaveAPI(tornado.web.RequestHandler):
    def get(self):
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        results = {
            "slaves": []
        }
        slaves_id = zk.get_children("/jetsearch/slaves")
        for slave_id in slaves_id:
            slave = zk.get("/jetsearch/slaves/" + slave_id)[0]
            results['slaves'].append(eval(str(slave)))
        zk.stop()
        tornado.escape.json_encode(results)
        self.write(results)