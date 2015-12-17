import os

import tornado
from tornado.options import define, options

from webui.modules.index import IndexHandler
from webui.modules.slaves import SlaveHandler

define("port", default=8000, help="run on the given port", type=int)

if __name__ == '__main__':
    tornado.options.parse_command_line()

    app = tornado.web.Application([
        ('/', IndexHandler),
        ('/slaves', SlaveHandler)
    ],
        template_path=os.path.join(os.path.dirname(__file__), "templates"),
        static_path=os.path.join(os.path.dirname(__file__), "static"),
        debug=True
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    print "start server on [0.0.0.0:8000]"
    tornado.ioloop.IOLoop.instance().start()