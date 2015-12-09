class Queue(object):
    def __init__(self, server, key):
        self.server = server
        self.key = key

    def __len__(self):
        raise NotImplementedError

    def push(self, value):
        raise NotImplementedError

    def pop(self):
        raise NotImplementedError

    def clear(self):
        self.server.delete(self.key)


class FIFOQueue(Queue):
    def __len__(self):
        return self.server.llen(self.key)

    def push(self, value):
        self.server.lpush(self.key, value)

    def pop(self):
        value = self.server.rpop(self.key)
        if value:
            return value
