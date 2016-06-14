import numpy as np
import pandas as pd
import seaborn as sns

from lib import db
from lib import cli
from pathlib import Path

args = cli.CommandLine('prediction-plot').args
root = Path(args.plotdir)

occurrences = [ 'SELECT as_of, node, jam',
                'FROM occurrence',
                'WHERE observation = {0} AND prediction = {1}',
                'GROUP BY node',
                ]

with db.DatabaseConnection() as con:
    with db.DatabaseCursor(con) as cursor:
        args = { 'x': 'as_of', 'y': 'jam' }
        # XXX should select all unique pairs
        sql = [ 'SELECT observation, prediction',
                'FROM occurrence',
                ]
        sql = db.process(sql)
        cursor.execute(sql)
        
        for row in cursor:
            windows = [ row[x] for x in [ 'observation', 'prediction' ]]
            sql = db.process(occurrences, *windows)

            df = pd.read_sql_query(sql, con=con, index_col=[ 'as_of', 'node' ])
            df = df.unstack(level='node')
            df = df.resample(args.gfilter).sum()
            grouped = df.groupby(lambda x: x.hour).mean()
            grouped = grouped.stack(level='node')

            args['data'] = grouped
            sns.boxplot(palette="PRGn", whis=np.inf, **args)
            sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **args)
            sns.despine(offset=10, trim=True)

            fname = '-'.join(windows)
            relative = Path(root, fname).with_suffix('.png')
            plt.gcf().savefig(str(relative))
