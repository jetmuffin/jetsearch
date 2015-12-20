import redis
from kazoo.client import KazooClient

task = {
    "start_url": "http://www.hhu.edu.cn/",
    "allowed_domain": "seu.hhu.cn"
}
redis = redis.Redis()
redis.lpush("task:spider", {
    "url": "http://www.hhu.edu.cn/",
    "life": 5
})


zk = KazooClient(hosts="127.0.0.1")
zk.start()
zk.create("/jetsearch/job", str(task))
zk.stop()

