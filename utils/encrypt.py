import hashlib


class Encrypt():
    @staticmethod
    def md5(string):
        md5 = hashlib.md5()
        md5.update(string)
        return md5.hexdigest()

    @staticmethod
    def sha1(string):
        sha1 = hashlib.sha1()
        sha1.update(string)
        return sha1.hexdigest()

