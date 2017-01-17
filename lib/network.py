import csv

class RoadNetwork(dict):
    def __init__(self, network_file):
        with network_file.open() as fp:
            for row in csv.reader(fp):
                (key, *row) = list(map(int, row))
                self[key] = row
    
    def bfs_(self, roots, trail):
        outgoing = []
        for i in filter(lambda x: x in self, roots):
            for j in filter(lambda x: x not in trail, self[i]):
                outgoing.append(j)
        
        if outgoing:
            yield outgoing
            yield from self.bfs_(outgoing, trail.union(outgoing))

    def bfs(self, root, inclusive=False):
        roots = [ root ]
        if inclusive:
            yield roots
        yield from self.bfs_(roots, set())
