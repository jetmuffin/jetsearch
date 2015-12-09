import redis
from pymongo import MongoClient
from kazoo.client import KazooClient

redis = redis.Redis()
redis.delete("set:duplicate")
redis.delete("task:spider")
redis.delete("task:processor")

mongodb = MongoClient(host="127.0.0.1", port=27017)
db = mongodb["jetsearch"]
db["page_docs"].remove()

zk = KazooClient(hosts="127.0.0.1")
zk.start()
if zk.exists("/jetsearch/task"):
    zk.delete("/jetsearch/task")
zk.stop()