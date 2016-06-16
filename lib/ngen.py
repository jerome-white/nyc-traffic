from lib import db

class NodeGenerator:
    def nodegen(self, *args):
        with db.DatabaseConnection() as conn:
            for (i, j) in enumerate(self.getnodes(conn)):
                tup = (i, j, args) if len(args) else (i, j)
                yield tup
                
    def getnodes(self, connection=None):
        if connection:
            with db.DatabaseCursor(connection) as cursor:
                yield from self._getnodes(cursor)
        else:
            with db.DatabaseConnection() as conn:
                yield from self.getnodes(conn)

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
    def __init__(self, table='operational'):
        self.table = table
        
    def _getnodes(self, cursor, order=False):
        sql = [ 'SELECT id',
                'FROM {0}',
                ]
        if order:
            sql.append('ORDER BY id ASC')

        cursor.execute(db.process(sql, self.table))
        for row in cursor:
            yield row['id']
