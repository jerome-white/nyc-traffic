from lib import db
from lib import logger
from lib import node as nd
from lib import cluster as cl
from itertools import chain

class Network:
    def __build(self, level, cluster, db_conn, seen):
        if level > 0:
            try:
                cl = cluster(self.node.nid, db_conn)
            except AttributeError as err:
                log = logger.getlogger()
                log.error(err)
                return
            
            neighbors = cl.neighbors.difference(seen)
            seen.update(neighbors)
            
            for i in neighbors:
                try:
                    lag = cl.lag(i) + self.lag
                except ValueError as err:
                    log = logger.getlogger()
                    log.error(err)
                    continue
                
                child = Network(i, level - 1, cluster, lag, db_conn, seen)
                self.children.add(child)

    def __init__(self, nid, level, cluster, lag=0, db_conn=None, seen=None):
        self.lag = lag
        self.children = set()

        close = not db_conn
        if close:
            db_conn = db.DatabaseConnection().resource
            
        self.node = nd.Node(nid, db_conn)
        if not seen:
            seen = set()
            seen.add(nid)
        self.__build(level, cluster, db_conn, seen)
        
        if close:
            db_conn.close()

    def __repr__(self):
        return ' '.join(map(repr, [ x.node for x in self ]))
            
    def __str__(self):
        nodes = [ self ] + list(self.children)
        c = [ '{0}@{1}'.format(repr(x.node), x.lag) for x in nodes ]
        if len(c) > 1:
            c[0] += ':'
        c[-1] += ','

        c += [ str(x) for x in self.children ]
            
        return ' '.join(c)

    def __iter__(self):
        return next(self)

    def __next__(self):
        for i in self.children:
            yield from i
        yield self
   
    def depth(self):
        d = [ 0 ] + [ 1 + x.depth() for x in self.children ]
        return max(d)

    def nodes(self):
        for i in self:
            yield(i.node)
