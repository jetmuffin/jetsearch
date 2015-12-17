import redis
from kazoo.client import KazooClient

task = {
    "start_url": "http://www.hhu.edu.cn",
    "allowed_domain": "hhu.edu.cn"
}
redis = redis.Redis()
redis.lpush("task:spider", "http://www.hhu.edu.cn")


zk = KazooClient(hosts="127.0.0.1")
zk.start()
zk.create("/jetsearch/task", str(task))
zk.stop()

