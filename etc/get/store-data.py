import pickle

from lib import db
from lib import cli

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

#
# Open and parse the data file
#
with open(args.input, mode='rb') as fp:
    data = pickle.load(fp)
    
keys = []
values = []
for i in data:
    if not keys:
        keys = i.keys()
    values.append([ i[x] for x in keys ])
assert(keys and values)

#
# Create the SQL statement and execute!
#
s = [ '%s' ] * len(keys)
opts = [ ','.join(x) for x in (keys, s) ]
sql = [
    'INSERT IGNORE INTO reading ({0})',
    'VALUES ({1})'
]
sql = db.process(sql, opts)

db.EstablishCredentials(user='social')
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        # http://stackoverflow.com/a/18245311
        cursor.executemany(sql, values)
