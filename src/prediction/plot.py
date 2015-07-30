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

df = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
assert(all([ x in df.columns for x in user.args.gfilter]))
    
df = df.loc[df['confusion_matrix'] != np.nan]
grouped = df.groupby(user.args.gfilter + ['node'])['matthews_corrcoef']
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
args = { 'kind': 'scatter', 'x': 'cluster', 'y': 'mean' }

with CSVWriter(csvheader, user.args.stfile) as csv:
    csv.writeheader()
    for i in user.args.gfilter:
        for j in df[i].unique():
            cf = pd.DataFrame.from_csv(j)
            view = pd.merge(df_mean, cf, left_index=True, right_index=True)

            view.reset_index(level='node', drop=True, inplace=True)
            idxs = view.index.unique()
            for (x, y) in enumerate(idxs):
                view.loc[y, 'cluster'] += x / len(idxs)

                f = path.basename(i) + ext_
                fname = path.join(user.args.plotdir, f)

                ax = None
                args.update({
                    'xlim': (view.cluster.min(), view.cluster.max()),
                    'ax': ax,
                })
                assert(len(idxs) == len(colors))
        
                for (x, y) in zip(idxs, colors):
                    v = view.loc[x]
                    ax = v.plot(label=str(x), color=y, **args)
                    corr = v['mean'].corr(v['cluster'], method='pearson')
                    
                    row = [ f, j, corr ]
                    csv.writerow(dict(zip(csvheader, row)))
                assert(ax)
                utils.mkplot_(ax, fname)
