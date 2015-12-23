import logging
import logging.config
import time

from pymongo import MongoClient

from processor.chain_processor import CompressProcessor, PagerankProcessor, ReverseProcessor, RankingProcessor

logging.config.fileConfig('logging.conf')
root_logger = logging.getLogger('root')
mongo = MongoClient("127.0.0.1", 27017)
db = mongo.jetsearch

chain = [
    RankingProcessor(db)
]

start_time = time.time()

for processor in chain:
    processor.fire()

end_time = time.time()
print("Chain processor complete, took %f s" % (end_time-start_time))