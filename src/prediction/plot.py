import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as pyp

from lib import cli
from lib import utils

def combine(vals, clusters):
    lst = []
    for i in range(vals.ndim):
        m = vals[i].merge(clusters, left_index=True, right_index=True)
        lst.append(m)

    return lst

ext_ = '.pdf'
user = cli.CommandLine(cli.optsfile('prediction-plot'))

df = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
grouped = df.groupby(['neighbors', 'node'])['matthews_corrcoef']
vals = grouped.agg([np.mean, np.std])

cf = pd.DataFrame.from_csv(user.args.clusters)
dfcf = combine(vals.ix, cf)

for (i, j) in enumerate(dfcf):
    f = 'scatter-' + str(i) + ext_
    fname = os.path.join(user.args.output, f)
    plt = j.plot(kind='scatter', x='cluster', y='mean')
    utils.mkplot_(plt, fname)
(x, y) = dfcf
 
# plot the zero/1-level neighbor comparison
z = pd.merge(x, y, left_index=True, right_index=True)
z = z.sort('mean_x', ascending=False)
fname = os.path.join(user.args.output, 'compare' + ext_)
fig = z[['mean_x', 'mean_y']]
err = z[['std_x', 'std_y']]
plt = fig.plot(figsize=(120, 20), fontsize=36, kind='bar', yerr=err,
               ylim=(-1, 1))
utils.mkplot_(plt, fname)
 
# plot the zero-neighbors in ascending order
s = x.sort('mean')#.groupby(['cluster'])
fname = os.path.join(user.args.output, 'zero' + ext_)
plt = s['mean'].plot(figsize=(40, 100), fontsize=36, kind='barh',
                     xerr=s['std'], xlim=(-1, 1))
plt.get_figure().savefig(fname)
pyp.close('all')

# plot the performance difference
fname = os.path.join(user.args.output, 'diff' + ext_)
diff = y['mean'] - x['mean']
diff.sort('mean', ascending=False)
plt = diff.dropna().plot(figsize=(100, 20), fontsize=36, kind='bar')
utils.mkplot_(plt, fname)
