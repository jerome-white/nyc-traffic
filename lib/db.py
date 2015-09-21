import pymysql

class Database:
    def __enter__(self):
        return self.resource

    def __exit__(self, type, value, tb):
        self.resource.close()
        
class DatabaseConnection(Database):
    def __init__(self, host='localhost', user='reader', db='traffic'):
        kwargs = {
            'host': host,
            'user': user,
            'db': db,
            'autocommit': True,
            'unix_socket': '/usr/local/mysql/data/mysql.sock',
        }
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

def process(sql, args=None):
    s = ' '.join(sql)
    
    return s.format(*args) if args else s
    
