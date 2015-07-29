import numpy as np
import pandas as pd
import os.path as path
import matplotlib.pyplot as pyp

from scipy import stats

from lib import cli
from lib import utils
from lib.csvwriter import CSVWriter

def plotter(fname, data, args):
    plt = data.plot(**args)
    utils.mkplot_(plt, fname)

ext_ = '.pdf'
user = cli.CommandLine(cli.optsfile('prediction-plot'))

vals = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
assert(all([ x in vals.columns for x in user.args.gfilter]))
    
df = vals.loc[vals['confusion_matrix'] != np.nan]
grouped = vals.groupby(user.args.gfilter + ['node'])['matthews_corrcoef']
df = grouped.agg([np.mean, stats.sem]).unstack(0)

#
# plot a comparison between the filters
#
fname = path.join(user.args.plotdir, 'compare' + ext_)
df = df.sort([('mean', df['mean'].columns[0])], ascending=False)
args = {
    'figsize': (400, 40),
    'fontsize': 96,
    'kind': 'bar',
    'yerr': df['sem'],
    'ylim': (0, 1),
    }
plotter(fname, df['mean'], args)

#
# plot the zero-neighbors in ascending order
#
# zsrt = zero.sort('mean')#.groupby(['cluster'])
# fname = path.join(user.args.plotdir, 'zero' + ext_)
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
args.update({ 'ylim': (-1, 1) })
df_mean = df['mean']
base = df_mean.columns[0]
for i in df_mean.columns[1:].tolist():
    f = 'diff.{0}-{1}{2}'.format(i, base, ext_)
    fname = path.join(user.args.plotdir, f)
    view = df_mean[i] - df_mean[base]
    view.sort(ascending=False)
    plotter(fname, view.dropna(), args)

exit()

colors = ['DarkBlue', 'DarkGreen', 'DarkRed']
csvheader = ['group', 'filter', 'correlation']
with CSVWriter(csvheader, user.args.stfile) as csv:
    csv.writeheader()
    for i in user.args.clusters:
        cf = pd.DataFrame.from_csv(i)
        view = pd.merge(df_mean, cf, left_index=True, right_index=True)

        view.reset_index(level='node', drop=True, inplace=True)
        idxs = view.index.unique()
        for (j, k) in enumerate(idxs):
            view.loc[k, 'cluster'] += j / len(idxs)

        f = path.basename(i) + ext_
        fname = path.join(user.args.plotdir, f)

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
            
            row = [ f, j, v['mean'].corr(v['cluster'], method='pearson') ]
            csv.writerow(dict(zip(csvheader, row)))
        assert(ax)
        utils.mkplot_(ax, fname)
