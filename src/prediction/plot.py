import numpy as np
import pandas as pd
import os.path as path
import matplotlib.pyplot as pyp

from lib import cli
from lib import utils

def plotter(fname, data, args):
    plt = data.plot(**args)
    utils.mkplot_(plt, fname)

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
fname = path.join(user.args.output, 'compare' + ext_)
view = z[['mean_x', 'mean_y']]
args = {
    'figsize': (120, 20),
    'fontsize': 36,
    'kind': 'bar',
    'yerr': z[['std_x', 'std_y']],
    'ylim': (-1, 1),
    }
plotter(fname, view, args)
 
# plot the zero-neighbors in ascending order
s = zero.sort('mean')#.groupby(['cluster'])
fname = path.join(user.args.output, 'zero' + ext_)
view = s['mean']
args = {
    'figsize': (40, 100),
    'fontsize': 36,
    'kind': 'barh',
    'xerr': s['std'],
    'xlim': (-1, 1),
    }
plotter(fname, view, args)

# plot the performance difference
fname = path.join(user.args.output, 'diff' + ext_)
view = one['mean'] - zero['mean']
view.sort('mean', ascending=False)
args = {
    'figsize': (100, 20),
    'fontsize': 36,
    'kind': 'bar',
    }
plotter(fname, view.dropna(), args)

colors = [ 'DarkBlue', 'DarkGreen' ]
for i in user.args.clusters:
    cf = pd.DataFrame.from_csv(i)
    view = pd.merge(vals, cf, left_index=True, right_index=True)

    view = view.reset_index(level='node', drop=True)
    view.loc[[1],'cluster'] += 0.5

    f = path.basename(i) + ext_
    fname = path.join(user.args.output, f)

    ax = None
    for j in range(view.ix.ndim):
        v = view.loc[[j]]
        ax = v.plot(kind='scatter', x='cluster', y='mean',
                    label=str(j), color=colors[j], ax=ax)

        print(f, j, v['mean'].corr(v['cluster'], method='pearson'))
    assert(ax)
    utils.mkplot_(ax, fname)
