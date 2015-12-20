import redis
from kazoo.client import KazooClient

task = {
    "start_url": "http://www.jetmuffin.com",
    "allowed_domain": "jetmuffin.com"
}
redis = redis.Redis()
redis.lpush("task:spider", {
    "url": "http://www.jetmuffin.com",
    "life": 5
})


zk = KazooClient(hosts="127.0.0.1")
zk.start()
zk.create("/jetsearch/job", str(task))
zk.stop()

