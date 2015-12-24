from mq.queue import FIFOQueue
import redis

redis = redis.Redis()
spider_queue = FIFOQueue(redis, "task:spider")
while len(spider_queue):
    test = eval(spider_queue.pop())
    if "apple.com" in test['url']:
        print test['url']
        continue
    spider_queue.push(str(test))
