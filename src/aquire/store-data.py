import pickle

import pandas as pd

from lib import db
from lib import cli

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

columns = [ 'Id': 'id', 'Speed': 'speed', 'TravelTime':  ]

#
# Open and parse the data file
#
df = pd.DataFrame.from_csv(args.input, sep='\t', index_col=[ 'DataAsOf' ])
assert(len(df) > 0)

df.index.name = 'as_of'
df = df.ix[df.index.max()]
df = df[columns]

#
# Create the SQL statement
#
cols = df.columns.values
opts = [ ','.join(x) for x in ( cols, [ '%s' ] * len(cols) )]
sql = [
    'INSERT IGNORE INTO reading ({0})',
    'VALUES ({1})'
]

#
# Add to the database!
#
db.EstablishCredentials(user='social')
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        cursor.executemany(db.process(sql, *opts), records.tolist())
