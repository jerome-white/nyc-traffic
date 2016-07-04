import pickle

import pandas as pd

from lib import db
from lib import cli

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

#
# Open and parse the data file
#
with open(args.input, mode='rb') as fp:
    data = pickle.load(fp)
df = pd.DataFrame.from_records(data)
assert(len(df) > 0)
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

#
# Add to the database!
#
db.EstablishCredentials(user='social')
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        cursor.executemany(db.process(sql, *opts), records.tolist())
