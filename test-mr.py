import logging
import logging.config
from pymongo import MongoClient
from bson import Code

from master.scheduler import Scheduler
logging.config.fileConfig('logging.conf')
root_logger = logging.getLogger('root')
root_logger.debug('test root logger...')

scheduler = Scheduler()
scheduler.chain_process()

