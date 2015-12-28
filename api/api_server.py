import tornado
import tornado.web
import tornado.httpserver
import logging

from api.api_handlers import QueryAPI, SlaveAPI

logger = logging.getLogger("root.APIServer")


class APIServer(object):
    def __init__(self):
        self.api = tornado.web.Application([
            ("/query", QueryAPI),
            ("/slaves", SlaveAPI)
        ],
                debug=True
        )

    def start(self):
        http_server = tornado.httpserver.HTTPServer(self.api)
        http_server.listen(8000)
        logger.info("[API] api server start on port 8000.")
        tornado.ioloop.IOLoop.instance().start()
