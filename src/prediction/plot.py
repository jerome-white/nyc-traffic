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

df = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None).dropna()
grouped = df.groupby(user.args.gfilter + ['node'])['matthews_corrcoef']
vals = grouped.agg([np.mean, np.std])

for i in vals.index.levels[:-1]:
    args = { 'level': user.args.gfilter, 'drop': True }
    splt = [ vals.loc[x].reset_index(**args) for x in i ]
(zero, one) = splt

#
# plot a comparison between the filters
#
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

#
# plot the zero-neighbors in ascending order
#
# zsrt = zero.sort('mean')#.groupby(['cluster'])
# fname = path.join(user.args.output, 'zero' + ext_)
# view = zsrt['mean']
# args = {
#     'figsize': (40, 100),
#     'fontsize': 36,
#     'kind': 'barh',
#     'xerr': zsrt['std'],
#     'xlim': (-1, 1),
#     }
# plotter(fname, view, args)

#
# plot the performance difference
#
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

    view.reset_index(level='node', drop=True, inplace=True)
    idxs = view.index.unique()
    for (j, k) in enumerate(idxs):
        view.loc[k,'cluster'] += j / len(idxs)

    f = path.basename(i) + ext_
    fname = path.join(user.args.output, f)

    ax = None
    args = {
        'kind': 'scatter',
        'x': 'cluster',
        'y': 'mean',
        'xlim': (view.cluster.min(), view.cluster.max()),
        'ax': ax,
        }
    assert(len(idxs) == len(colors))
    for (j, k) in zip(idxs, colors):
        v = view.loc[j]
        ax = v.plot(label=str(j), color=k, **args)

        print(f, j, v['mean'].corr(v['cluster'], method='pearson'))
    assert(ax)
    utils.mkplot_(ax, fname)
