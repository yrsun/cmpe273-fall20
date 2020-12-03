from hashlib import md5
import bisect

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
        pos = bisect.bisect(self.hnodes, self.hash(repr(val)))
        if pos == len(self.hnodes):
            return self.nodes_map[self.hnodes[0]]
        else:
            return self.nodes_map[self.hnodes[pos]]

    def del_node(self, val):
        index = bisect.bisect_left(self.hnodes, self.hash(val))
        newNode = self.nodes_map[self.hnodes[0]]
        if ((index + 1) < len(self.hnodes)):
            newNode = self.nodes_map[self.hnodes[index + 1]]
        del self.nodes_map[self.hnodes[index]]
        self.hnodes.remove(self.hash(val))
        return newNode

    def add_node(self, val):
        key = self.hash(val)
        bisect.insort(self.hnodes, key)
        self.nodes_map[key] = val
        index = bisect.bisect(self.hnodes, key)
        if index >= len(self.hnodes):
            index = 0
        return self.nodes_map[self.hnodes[index]]
