import tornado


class TaskHandler(tornado.web.RequestHandler):
    def post(self):
        start_url = self.get_argument("start_url")
        allowed_domain = self.get_argument("allowed_domain")
        print start_url, allowed_domain

    def get(self):

        self.render(
            'tasks.html',
            page_title="Tasks",
        )