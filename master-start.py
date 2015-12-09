# -*- coding: utf-8 -*-
from optparse import OptionParser
import sys
import re
from time import sleep
import logging
import logging.config
logging.config.fileConfig('logging.conf')
root_logger = logging.getLogger('root')
root_logger.debug('test root logger...')

from master.scheduler import Scheduler


def address_validate(addr, name):
    if not addr:
        print_error("Missing required parameters: %s address required." % name)
    else:
        pattern = re.compile(r'[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{2,5}')
        if not pattern.match(addr):
            print_error("Wrong address format: %s address format illegal" % name)


def print_error(message):
    print >> sys.stderr, message
    print >> sys.stderr, "Use --help to show usage."
    exit(2)


if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-z", "--zk", help="zookeeper address <host:port>", dest="zk")
    parser.add_option("-r", "--redis", help="redis address <host:port>", dest="redis")
    parser.add_option("-m", "--mongo", help="mongodb address <host:port>", dest="mongodb")

    (options, args) = parser.parse_args()

    """ 参数验证 """
    address_validate(options.zk, "zookeeper")
    address_validate(options.mongodb, "mongodb")
    address_validate(options.redis, "redis")

    master = Scheduler(options.zk, options.redis, options.mongodb)
    while True:
        sleep(2)
