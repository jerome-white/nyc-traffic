import pickle

import pandas as pd

from lib import db
from lib import cli

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

index_col = 'DataAsOf'
columns = { index_col: 'as_of',
            'Id': 'node',
            'Speed': 'speed',
            'TravelTime': 'travel_time',
            }

#
# Open and parse the data file
#
df = pd.read_csv(args.input, sep='\t', index_col=index_col, parse_dates=True)
df = df.rename(index=str).reset_index().rename(columns=columns)

df = df[list(columns.values())]
records = df.to_records(index=False)

#
# Create the SQL statement
#
cols = df.columns.values
opts = [ ','.join(x) for x in ( cols, [ '%s' ] * len(cols) )]
sql = [
    'INSERT IGNORE INTO reading ({0})',
    'VALUES ({1})'
]
sql = db.process(sql, *opts)

#
# Add to the database!
#
db.EstablishCredentials(user='social')
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        cursor.executemany(sql, records.tolist())
