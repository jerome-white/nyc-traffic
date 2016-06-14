import numpy as np
import pandas as pd
import seaborn as sns

from lib import db
form lib import cli

args = cli.CommandLine('prediction-plot').args

sql = [ 'SELECT as_of, node, jam',
        'FROM occurrence',
        'WHERE observation = {0} AND prediction = {1}',
        'GROUP BY node',
        ]
sql = db.process(sql, args.window_obs, args.window_pred)

with db.DatabaseConnection() as con:
    df = pd.read_sql_query(sql, con=con, index_col=[ 'as_of', 'node' ])
    df = df.unstack(level='node')
    df = df.resample(args.gfilter).mean()
grouped = df.groupby(lambda x: x.hour).mean()
grouped = grouped.stack(level='node')

args = { 'x': 'as_of', 'y': 'jam', 'data': grouped }
sns.boxplot(palette="PRGn", whis=np.inf, **args)
sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **args)
sns.despine(offset=10, trim=True)

plt.gcf().savefig(args.plotname)
