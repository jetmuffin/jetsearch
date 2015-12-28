# -*- coding: utf-8 -*-
import os
from optparse import OptionParser
import sys
import re
from time import sleep
import logging
import logging.config
logging.config.fileConfig('logging.conf')
root_logger = logging.getLogger('root')

from master.scheduler import Scheduler


def print_error(message):
    print >> sys.stderr, message
    print >> sys.stderr, "Use --help to show usage."
    exit(2)


if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-c", "--config", help="configuration path", dest="config_path")

    (options, args) = parser.parse_args()

    """ 参数验证 """
    if options.config_path:
        if not os.path.exists(options.config_path):
            print_error("Configuration path error: file not exists.")
    else:
        print_error("Configuration required")

    master = Scheduler(options.config_path)
    master.listen()