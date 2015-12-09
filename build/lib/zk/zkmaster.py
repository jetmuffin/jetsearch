from kazoo.client import KazooClient

class ZKMaster(object):
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()

    def list_slaves(self):
        slaves = self.zk.get_children("/jetsearch/slaves")
        return slaves

    def slave_status(self, slave_id):
        slave = self.zk.get("/jetsearch/slaves" + slave_id)
        return slave

    def __del__(self):
        self.zk.stop()
