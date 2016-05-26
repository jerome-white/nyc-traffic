from collections import defaultdict

from lib import db

class NeighborStrategy:
    def __init__(self, connection=None):
        self.connection = connection

    def get_neighbors(self, nid):
        manual = not self.connection
        c = db.DatabaseConnection().resource if manual else self.connection
            
        with db.DatabaseCursor(c) as cursor:
            cursor.execute(self.get_sql(nid))
            return frozenset([ row['nid'] for row in cursor ])

        if manual:
            c.close()
        
    def get_sql(self, nid):
        raise NotImplementedError()

class SpatialNeighbors(NeighborStrategy):
    def __init__(self, connection=None):
        super().__init__(connection)
        
        self.fmt = defaultdict(str)
        
    def get_sql(self, nid):
        self.fmt['nid'] = nid
        sql = [ 'SELECT target.id AS nid',
                'FROM operational source, operational target',
                'WHERE {1}INTERSECTS(source.segment, target.segment)',
                'AND source.id = {0} AND target.id <> {0} {2}',
                ]
        params = [ self.fmt[x] for x in ('nid', 'geo', 'order') ]
        
        return db.process(sql, params)

class STNeighbors(SpatialNeighbors):
    def __init__(self, connection=None):
        super().__init__(connection)
    
        st_fun = 'ST_DISTANCE(target.segment, source.segment)'
        self.fmt['order'] = 'ORDER BY ' + st_fun
        self.fmt['geo'] = 'ST_'

class MBRNeighbors(SpatialNeighbors):
    def __init__(self, connection=None):
        super().__init__(connection)

        self.fmt['geo'] = 'MBR'

class StaticNeighbors(NeighborStrategy):
    def __init__(self, connection=None, direction='incoming'):
        super().__init__(connection)

        self.columns = [ 'source', 'target' ]
        if direction != 'incoming':
            self.columns = self.columns[::-1]
        
    def get_sql(self, nid):
        sql = [ 'SELECT n.{1} AS nid',
                'FROM network n',
                'LEFT JOIN operational o ON n.{1} = o.id',
                'WHERE n.{2} = {0} AND o.id IS NOT NULL',
                ]

        return db.process(sql, [ nid ] + self.columns)
