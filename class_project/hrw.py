from hashlib import md5

class HRW(object):
    def __init__(self, server_list):
        self.server_list = server_list

    def hash(self, val):
        m = md5(val.encode())
        return int(m.hexdigest(), 16)%(2**32)

    def get_node(self, val):
        highest_score, champion = -1, None
        for node in self.server_list:
            score = self.hash(node+str(val))
            if score > highest_score:
                champion, highest_score = node, score
        return champion
