from mq.queue import FIFOQueue
import redis

redis = redis.Redis()
spider_queue = FIFOQueue(redis, "task:spider")
while len(spider_queue):
    test = spider_queue.pop()
    if "hhu.edu.cn" in test:
        # print "YES"
        spider_queue.push(test)
