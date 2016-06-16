import pymysql

from collections import namedtuple

Credentials = namedtuple('Credentials', [ 'host', 'user', 'db' ])
class EstablishCredentials:
    credentials = None
    
    def __new__(self, host='localhost', user='reader', db='traffic'):
        if not self.credentials:
            self.credentials = Credentials(host, user, db)

        return self.credentials

class Database:
    def __enter__(self):
        return self.resource

    def __exit__(self, type, value, tb):
        self.resource.close()
        
class DatabaseConnection(Database):
    def __init__(self):
        kwargs = {
            'autocommit': True,
            # 'unix_socket': '/usr/local/mysql/data/mysql.sock',
        }

        credentials = EstablishCredentials()
        kwargs.update(credentials._asdict())
        
        self.resource = pymysql.connect(**kwargs)

class DatabaseCursor(Database):
    def __init__(self, connection):
        self.resource = connection.cursor(pymysql.cursors.DictCursor)

def mark():
    with DatabaseConnection() as connection:
        with DatabaseCursor(connection) as cursor:
            cursor.execute('SELECT MAX(as_of) AS mark FROM reading')
            row = cursor.fetchone()
            
            return row['mark']

def process(sql, *args):
    s = ' '.join(sql)

    return s.format(*args) if args else s

#
# Generate the "operational" table, which is a filtered view of the
# node table. It contains a subset of segments that we want to
# consider.
#
def genop(frequency=0, table='operational'):
    # First delete existing values...
    sql = [[ 'DELETE FROM {0}' ]]

    # ... then insert new values
    insert = [ 'INSERT INTO {0} (id, name, segment)',
               'SELECT n.id, n.name, n.segment',
               'FROM node AS n',
    ]
    if frequency > 0:
        join = [ 'JOIN quality AS q ON n.id = q.node',
                 'WHERE q.frequency <= {0}'.format(frequency)
        ]
        insert.extend(join)
    sql.append(insert)

    with DatabaseConnection() as connection:
        with DatabaseCursor(connection) as cursor:
            for i in sql:
                statement = process(i, [ table ])
                cursor.execute(statement)
