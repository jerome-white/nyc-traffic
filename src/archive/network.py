import sys
import csv

from lib import db
from collections import defaultdict

columns = [ 'source', 'target' ]
sql = [ 'SELECT {0}',
        'FROM network'
]
sql = db.process(sql, ','.join(columns))

network = defaultdict(list)
with db.DatabaseConnection() as connection:
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
        for row in cursor:
            (source, target) = map(int, map(lambda x: row[x], columns))
            network[target].append(source)

writer = csv.writer(sys.stdout)
writer.writerows([ [ x ] + y for (x, y) in network.items() ])
