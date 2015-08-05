import os

import numpy as np
import pandas as pd

from scipy import stats
from pathlib import Path

from lib import cli
from lib import utils
# from lib.csvwriter import CSVWriter

def plotter(fname, data, args):
    plt = data.plot(**args)
    utils.mkplot_(plt, fname)

def comparison(df, args, output_d, ext='pdf'):
    '''
    plot a comparison between the filters
    '''
    fname = path.join(output_d, '.'.join([ 'compare', ext ]))
    df = df.sort([('mean', df['mean'].columns[0])], ascending=False)
    plotter(fname, df['mean'], args)

def performance(df, args, output_d, ext='pdf'):
    '''
    plot the performance difference
    '''
    args.update({ 'ylim': (-1, 1) }) # XXX this should be a copy
    df_mean = df['mean']
    base = df_mean.columns[0]
    for i in df_mean.columns[1:].tolist():
        f = 'diff.{0}-{1}{2}'.format(i, base, ext)
        fname = path.join(output_d, f)
        view = df_mean[i] - df_mean[base]
        view.sort(ascending=False)
        plotter(fname, view.dropna(), args)

def anova(df, cluster_directory):
    def issig(val, sig=0.05):
        return '*' if val < sig else ' '
    
    for i in df.columns:
        args = [ str(x) for x in (10, i, 5, -0.002) ]

        path = Path(cluster_directory, *args)
        for pth in path.iterdir():
            if pth.suffix != '.csv':
                continue
            data = pd.DataFrame.from_csv(str(pth))
            cf = pd.concat((df[i], data), axis=1).dropna()
            groups = cf.groupby(['cluster'])[i]

            samples = [ x.values for (_, x) in groups ]
            oneway = stats.f_oneway(*samples)
            kruskal = stats.kruskal(*samples)

            msg = '{0:2d} {1:2d} '.format(i, len(groups))
            # msg = '{0} {1:2d} '.format(i, len(groups))
            for (v, p) in (oneway, kruskal):
                msg += '{0:6.3f} {1:.3f} {2} '.format(v, p, issig(p))
            print(msg)
    
user = cli.CommandLine(cli.optsfile('prediction-plot'))

raw = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
assert(all([ x in raw.columns for x in user.args.gfilter]))
raw = raw.loc[raw['confusion_matrix'] != np.nan]

grouped = raw.groupby(user.args.gfilter + ['node'])['matthews_corrcoef']
df = grouped.agg([np.mean, stats.sem]).unstack(0)

args = {
    'figsize': (400, 40),
    'fontsize': 96,
    'kind': 'bar',
    'yerr': df['sem'],
    'ylim': (0, 1),
}
# comparison(df, args, user.args.plotdir)
# performance(df, args, user.args.plotdir)

anova(df['mean'], user.args.clusters)
