import pymysql

class Database:
    def __enter__(self):
        return self.resource

    def __exit__(self, type, value, tb):
        self.resource.close()
        
class DatabaseConnection(Database):
    def __init__(self, host='localhost', user='reader', db='traffic'):
        kwargs = { 'host': host, 'user': user, 'db': db, 'autocommit': True }
        self.resource = pymysql.connect(**kwargs)

class DatabaseCursor(Database):
    def __init__(self, connection):
        self.resource = connection.cursor(pymysql.cursors.DictCursor)
