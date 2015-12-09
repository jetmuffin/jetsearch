# -*- coding: utf-8 -*-
from optparse import OptionParser
import sys
import re

def print_error(message):
    print >> sys.stderr, message
    print >> sys.stderr, "Use --help to show usage."
    exit(2)

if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [options]")
    parser.add_option("-z", "--zk", help="zookeeper address <host:port>", dest="zk")
    parser.add_option("-r", "--redis", help="redis address <host:port>", dest="redis")

    (options, args) = parser.parse_args()

    """ 参数验证 """
    if not options.zk:
        print_error("Missing required parameters: zookeeper address required.")
    else:
        pattern = re.compile(r'[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{2,5}')
        if not pattern.match(options.zk):
            print_error("Wrong address format: zookeeper address format illegal")

    if not options.redis:
        print_error("Missing required parameters: redis address required.")
    else:
        pattern = re.compile(r'[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{2,5}')
        if not pattern.match(options.redis):
            print_error("Wrong address format: redis address format illegal")
