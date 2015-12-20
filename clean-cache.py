import redis
from pymongo import MongoClient
from kazoo.client import KazooClient

redis = redis.Redis()
redis.delete("set:duplicate")
redis.delete("task:spider")
redis.delete("task:processor")


mongodb = MongoClient(host="127.0.0.1", port=27017)
db = mongodb["jetsearch"]
db["tbl_page"].remove()
db["tbl_doc"].remove()
db["tbl_term"].remove()
db["tbl_pagerank"].remove()

zk = KazooClient(hosts="127.0.0.1")
zk.start()
if zk.exists("/jetsearch/job"):
    zk.delete("/jetsearch/job")
zk.stop()