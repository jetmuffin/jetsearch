# -*- coding: utf-8 -*-
import urllib2
from pprint import pprint

from spiders.async_crawlers import Fetcher
from spiders.parser import NormalParser


def log(message):
    print message


class Crawler(object):
    def __init__(self, job, config=None):
        self.job = job
        self.config = config
        self.header = {
            "User-Agent": "Mozilla-Firefox5.0"
        }

    def fetch(self, url):
        raise NotImplementedError

    def parse(self, url, html):
        """
        解析抓取的HTML文件,抽取信息
        """
        raise NotImplementedError

    def storage(self):
        raise NotImplementedError


class SimpleCrawler(Crawler):
    """ 简单爬虫 """

    def fetch(self, url):
        """
        发送http请求并获取HTML文本
        :param url: 需要抓取的url
        """
        request = urllib2.Request(url)
        for key in self.header:
            request.add_header(key, self.header[key])
        try:
            response = urllib2.urlopen(request, timeout=15)
            return True, response.read()
        except Exception, e:
            return False, e

    def parse(self, url, html):
        parser = NormalParser(url, html, self.job)
        return parser.parse()

job = {
    "allowed_domain": "hhu.edu.cn",
    "start_url": "http://www.hhu.edu.cn"
}
