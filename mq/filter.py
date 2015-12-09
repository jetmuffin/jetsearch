# -*- coding: utf-8 -*-

class BaseDupeFilter:
    def __init__(self, server, key):
        self.server = server
        self.key = key

    def exists(self, url):
        raise NotImplementedError

    def close(self):
        self.clear()

    def clear(self):
        self.server.delete(self.key)


class DuplicateFilter(BaseDupeFilter):
    """
    重复过滤器
    """

    def exists(self, url):
        if self.server.sismember(self.key, url):
            return True
        self.server.sadd(self.key, url)
        return False
