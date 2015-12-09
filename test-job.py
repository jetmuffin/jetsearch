import redis
from kazoo.client import KazooClient

task = {
    "start_url": "http://www.sunzequn.com",
    "allowed_domain": "www.sunzequn.com"
}
redis = redis.Redis()
redis.lpush("task:spider", "http://www.sunzequn.com")


zk = KazooClient(hosts="127.0.0.1")
zk.start()
zk.create("/jetsearch/task", str(task))
zk.stop()

