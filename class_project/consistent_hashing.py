from hashlib import md5
from bisect import bisect

class Ring(object):
    def __init__(self, server_list):
        hnodes = [self.hash(server) for server in server_list]
        hnodes.sort()
        self.hnodes = hnodes
        self.nodes_map = {self.hash(server) : server for server in server_list}

    def hash(self, val):
        m = md5(val.encode())
        return int(m.hexdigest(), 16)%(2**32)

    def get_node(self, val):
        pos = bisect(self.hnodes, self.hash(repr(val)))
        if pos == len(self.hnodes):
            return self.nodes_map[self.hnodes[0]]
        else:
            return self.nodes_map[self.hnodes[pos]]
