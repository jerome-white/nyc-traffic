from lib import db

class NodeGenerator:
    def nodegen(self, *args):
        with db.DatabaseConnection() as conn:
            for (i, j) in enumerate(self.getnodes(conn)):
                tup = (i, j, args) if len(args) else (i, j)
                yield tup
                
    def getnodes(self, connection=None):
        if not connection:
            with db.DatabaseConnection() as conn:
                yield from self.getnodes(conn)
            return
        
        with db.DatabaseCursor(connection) as cursor:
            yield from self._getnodes(cursor)

    def _getnode(self, cursor):
        raise NotImplementedError()

class ParallelGenerator(NodeGenerator):
    def __init__(self, frequency=None):
        if frequency >= 0:
            db.genops(frequency)
            
    def _getnodes(self, cursor):
        result = '@id'
        
        while True:
            cursor.execute('CALL getnode({0})'.format(result))
            cursor.execute('SELECT {0}'.format(result))
            row = cursor.fetchone()
            if not row[result]:
                break

            yield row[result]

class SequentialGenerator(NodeGenerator):
    def _getnodes(self, cursor):
        sql = [ 'SELECT id',
                'FROM operational',
                'ORDER BY id ASC',
                ]
        
        cursor.execute(db.process(sql))
        for row in cursor:
            yield row['id']