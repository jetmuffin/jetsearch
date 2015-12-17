# -*- coding: utf-8 -*-
import urllib2

import re

from bs4 import BeautifulSoup
import urlparse
import time
class SimpleCrawler:
    def __init__(self, start_url, allowed_domain):
        """
        简单爬虫
        :param start_url: 起始url
        :param allowed_domain: 允许域名范围
        """
        self.start_url = start_url
        self.allowed_domain = allowed_domain
        self.header = {
            "User-Agent": "Mozilla-Firefox5.0"
        }

    def fetch(self, url):
        """
        发送http请求并获取HTML文本
        :param url: 需要抓取的url
        """
        self.url = url_encode(url)
        request = urllib2.Request(self.url)
        for key in self.header:
            request.add_header(key, self.header[key])
        try:
            response = urllib2.urlopen(request, timeout=15)
            self.content = response.read().encode("utf-8")
            self.success = True
        except Exception, e:
            self.error = e
            self.success = False

    def parse(self):
        """
        解析抓取的HTML文件,抽取信息
        """
        if not self.url:
            print "Cannot parse before fetch!"
            raise RuntimeError

        soup = BeautifulSoup(self.content, "lxml")
        item = {
            "title": self.extract_title(soup),
            "content": self.extract_content(soup),
            "href": self.url,
            "links": self.extract_links(soup),
            "link_contents": self.extract_link_contents(soup),
            # "raw_content": self.content
        }
        return item

    def extract_title(self, soup):
        """
        抓取title内容
        :param soup: beautifulsoup对象
        """
        if soup.title:
            return soup.title.string
        else:
            return None
        # return soup.title.string

    def extract_content(self, soup):
        """
            extract main content of html
            :param soup:
        """

        # 去除JS脚本和CSS内容
        [script.extract() for script in soup.findAll('script')]
        [style.extract() for style in soup.findAll('style')]
        soup.prettify()

        reg = re.compile("<[^>]*>")
        content = reg.sub('', soup.prettify()).strip()
        content = " ".join(content.split())
        return content

    def extract_link_contents(self, soup):
        """
        抓取所有链接中的文本
        :param soup:
        """
        soup_url_list = soup.find_all("a")
        link_contents = []
        for soup_url in soup_url_list:
            if soup_url.string:
                link_contents.append(soup_url.string)

        return link_contents

    def extract_links(self, soup):
        """
        抓去页面中所有链接信息
        :param soup:
        """
        next_urls = []
        soup_url_list = soup.find_all("a")

        for soup_url in soup_url_list:
            url = soup_url.get("href")

            # 过滤包含邮箱的url
            if url_filter(url):
                if self.allowed_domain in url:
                    next_urls.append(url)

                elif len(url) and url[0] == '/':
                    # 处理相对路径
                    url = urlparse.urljoin(self.url, url)
                    next_urls.append(url)

        return next_urls

def url_filter(url):
    if not url:
        return False
    # 过滤含指定子串的url
    invalid_pattern = ["pdf", "mailto", "doc", "docx", "ppt", "pptx", "xls", "xlsx",
                       "jpg", "gif", "png", "zip", "rar"]
    for pattern in invalid_pattern:
        if pattern in url:
            return False
    return True

def url_encode(str):
    repr_str = repr(str).replace(r'\x', '%')
    return repr_str[1:-1]


print url_filter("http://cies.hhu.edu.cn/picture/article/36/c0/db/aa3cddc84166b5bade0d766d3c16/6ea40dfa-35e3-4564-9c10-8e938c98a8ea.pdf")
