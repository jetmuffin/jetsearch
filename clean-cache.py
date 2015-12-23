import redis
from pymongo import MongoClient
from kazoo.client import KazooClient

redis = redis.Redis(host="121.248.199.55", port=6379)
redis.delete("set:duplicate")
redis.delete("task:spider")
redis.delete("task:processor")


mongodb = MongoClient(host="121.248.199.55", port=20000)
db = mongodb["jetsearch"]
db["tbl_page"].remove()
db["tbl_doc"].remove()
db["tbl_term"].remove()
db["tbl_pagerank"].remove()

zk = KazooClient(hosts="121.248.199.55")
zk.start()

if zk.exists("/jetsearch/job"):
    zk.delete("/jetsearch/job")
zk.stop()