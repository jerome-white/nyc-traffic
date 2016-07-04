from lib import db

#
# Removes values prior to 7 November 2014. Such values will
# occasionally be reported, which are erroneous.
#
sql = [
    'DELETE FROM reading',
    "WHERE as_of < '{0}'",
]
sql = db.process(sql, '2014-11-07')

db.EstablishCredentials(user='social')
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
