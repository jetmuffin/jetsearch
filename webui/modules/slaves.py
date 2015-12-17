import tornado.web
from kazoo.client import KazooClient

class SlaveHandler(tornado.web.RequestHandler):
    def get(self):
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        slaves = []
        slaves_id = zk.get_children("/jetsearch/slaves")
        for slave_id in slaves_id:
            slave = zk.get("/jetsearch/slaves/" + slave_id)[0]
            slaves.append(eval(str(slave)))
        zk.stop()
        self.render(
            'slaves.html',
            slaves=slaves,
            page_title="Slaves",
        )
