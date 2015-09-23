
import lib.node as nd
import lib.cluster as cl

from lib import db
from lib.logger import log

class Network:
    def __init__(self, nid, lag=0, connection=None):
        self.node = nd.Node(nid, connection)
        self.lag = lag
        self.children = set()

    def __str__(self):
        network = [ self ] + list(self.children)
        c = [ '{0}@{1}'.format(repr(x.node), x.lag) for x in network ]
        if len(c) > 1:
            c[0] += ':'
        c[-1] += ','

        c += [ str(x) for x in self.children ]
            
        return ' '.join(c)
    
    def __flatten(self, root, align_and_shift):
        if root.node != self.node:
            self.node.readings.shift(root.lag)
            self.node.align(root.node, True)

        view = [ self.node ]
        for i in self.children:
            kids = i.__flatten(root, align_and_shift)
            view.extend(kids)

        return view
       
    def flatten(self, align_and_shift=True):
        return self.__flatten(self, align_and_shift)

    def depth(self):
        return max([ 1 + x.depth() for x in self.children ]) if self.children else 0
    
def neighbors_(parent, levels, cluster, conn, seen):
    if levels < 1 or parent in seen:
        return
    
    try:
        cl = cluster(parent.node.nid, conn)
    except AttributeError as err:
        log.error(err)
        return

    seen.add(parent.node.nid)
    for i in cl.neighbors.difference(seen):
        try:
            lag = cl.lag(i) + parent.lag
        except ValueError as err:
            log.error(err)
            continue

        net = Network(i, lag, conn)
        neighbors_(net, levels - 1, cluster, conn, seen)
        
        log.debug('parent {0} adding {1}'.format(repr(parent.node), repr(net.node)))
        parent.children.add(net)
            
def neighbors(source, levels, cluster, conn, align_and_shift=True):
    net = Network(source, connection=conn)
    neighbors_(net, levels, cluster, conn, set())
    
    log.debug('neighbors: {0}'.format(net))

    if net.depth() < levels:
        raise ValueError(source)

    return net.flatten()

# node = 149
# with db.DatabaseConnection() as conn:
#     for i in range(8):
#         try:
#             val = neighbors(node, i, cl.Cluster, conn)
#         except ValueError as val:
#             pass
#         log.info('L{0}: {1}'.format(i, val))
