import threading
import time
from metrics.metric import Metric


class Health(threading.Thread):
    def __init__(self, zk, slave_id):
        threading.Thread.__init__(self)
        self.end = False
        self.status = None
        self.zk = zk
        self.slave_id = slave_id

    def update(self, status):
        self.staus = status

    def run(self):
        while not self.end:
            slave = eval(str(self.zk.get("/jetsearch/slaves/" + self.slave_id)[0]))
            slave['heartbeat'] = Metric.get_heartbeat()

            slave['task_status'] = self.status
            self.zk.set("/jetsearch/slaves/" + self.slave_id, str(slave))
            time.sleep(3)

    def stop(self):
        self.end = True

