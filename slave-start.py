# -*- coding: utf-8 -*-
import logging
import logging.config
from optparse import OptionParser
import sys
import re

from slave.processor_workers import ProcessorWorker
from slave.spider_workers import SpiderWorker, AsyncSpiderWorker

logging.config.fileConfig('logging.conf')
root_logger = logging.getLogger('root')
root_logger.debug('test root logger...')



def print_error(message):
    print >> sys.stderr, message
    print >> sys.stderr, "Use --help to show usage."
    exit(2)


if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-m", "--master", help="master/zookeeper address <host:port>", dest="master")
    parser.add_option("-t", "--type", help="slave type (spider or processor)", dest="type")

    (options, args) = parser.parse_args()

    """ 参数验证 """
    if not options.master:
        print_error("Missing required parameters: master address required.")
    else:
        pattern = re.compile(r'[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{2,5}')
        if not pattern.match(options.master):
            print_error("Wrong address format: master address format illegal")

    """ 默认类型为spider """
    if options.type == "processor":
        slave = ProcessorWorker(options.master, "processor")
    else:
        slave = AsyncSpiderWorker(options.master, "spider")

    try:
        slave.listen()
    except KeyboardInterrupt:
        slave.disconect()



