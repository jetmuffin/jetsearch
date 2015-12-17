import tornado
import redis
from master.scheduler import Scheduler
class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render(
            'index.html',
            page_title="Index",
        )

    def post(self):
        task = {
            "start_url": self.get_argument("start_url"),
            "allowed_domain": self.get_argument("allowed_domain")
        }

        scheduler = Scheduler(task)
        scheduler.run()
        self.redirect("/")