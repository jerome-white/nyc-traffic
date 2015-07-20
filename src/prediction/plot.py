import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as pyp

from lib import cli
from lib import utils

ext_ = '.pdf'
user = cli.CommandLine(cli.optsfile('prediction-plot'))

df = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
grouped = df.groupby(['neighbors', 'node'])['matthews_corrcoef']
vals = grouped.agg([np.mean, np.std])

splt = []
for i in range(vals.ix.ndim):
    segment = vals.loc[[i]].reset_index(level='neighbors', drop=True)
    splt.append(segment)
(zero, one) = splt

# plot the zero/1-level neighbor comparison
z = pd.merge(zero, one, left_index=True, right_index=True)
z = z.sort('mean_x', ascending=False)
fname = os.path.join(user.args.output, 'compare' + ext_)
fig = z[['mean_x', 'mean_y']]
args = {
    'figsize': (120, 20),
    'fontsize': 36,
    'kind': 'bar',
    'yerr': z[['std_x', 'std_y']],
    'ylim': (-1, 1),
    }
plt = fig.plot(**args)
utils.mkplot_(plt, fname)
 
# plot the zero-neighbors in ascending order
s = zero.sort('mean')#.groupby(['cluster'])
fname = os.path.join(user.args.output, 'zero' + ext_)
fig = s['mean']
args = {
    'figsize': (40, 100),
    'fontsize': 36,
    'kind': 'barh',
    'xerr': s['std'],
    'xlim': (-1, 1),
    }
plt = fig.plot(**args)
plt.get_figure().savefig(fname)
pyp.close('all')

# plot the performance difference
fname = os.path.join(user.args.output, 'diff' + ext_)
diff = one['mean'] - zero['mean']
diff.sort('mean', ascending=False)
args = {
    'figsize': (100, 20),
    'fontsize': 36,
    'kind': 'bar',
    }
plt = diff.dropna().plot(**args)
utils.mkplot_(plt, fname)

exit()
# XXX to be complete

for i in user.args.clusters:
    cf = pd.DataFrame.from_csv(i)
    vals = vals.merge(cf, left_index=True, right_index=True)
    for j in range(vals.ix.ndim):
        f = 'scatter-' + str(j) + ext_
        fname = os.path.join(user.args.output, f)

        view = vals.loc[[j],'mean']
        plt = view.plot(kind='scatter', x='cluster', y='mean')
        utils.mkplot_(plt, fname)
    
    vals.drop('cluster', axis=1, inplace=True)
