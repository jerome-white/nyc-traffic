import os
import operator

import numpy as np
import pandas as pd

from scipy import stats
from pathlib import Path

from lib import cli
from lib import utils
# from lib.csvwriter import CSVWriter

def padding(view, thresh=0.1):
    lst = []
    vals = view.dropna().values
    for i in (vals.min(), vals.max()):
        x = round(i, 1)
        if i < 0 and x >= i:
            lst.append(x - 0.1)
        elif i > 0 and x <= i:
            lst.append(x + 0.1)
        else:
            lst.append(x)

    return lst

def plotter(df, fname, ylabel, args):
    plt = df.dropna().plot(**args)
    fontdict = {
        'fontsize': args['fontsize'],
    } if 'fontsize' in args else None
    
    plt.set_xlabel('Traffic segments', fontdict=fontdict)
    plt.set_ylabel(ylabel, fontdict=fontdict)
    plt.set_ylim(padding(df))
    # plt.grid()
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom='on',       # ticks along the bottom edge are off
        top='off',         # ticks along the top edge are off
        labelbottom='off') # labels along the bottom edge are off
    
    utils.mkplot_(plt, fname)

def distribution(df, about=0):
    lst = []
    for i in (operator.gt, operator.eq, operator.lt):
        lst.append([ i.__name__, len(df[i(df, about)]) ])

    return lst

def comparison(df, args, output_d, ext='pdf'):
    '''
    plot a comparison between the filters
    '''
    fname = os.path.join(output_d, '.'.join([ 'compare', ext ]))
    view = df.sort([('mean', df['mean'].columns[0])], ascending=False)

    plotter(view['mean'], fname, 'MCC', args)

def performance(df, args, output_d, ext='pdf'):
    '''
    plot the performance difference
    '''
    
    df_mean = df['mean']
    for source in df_mean.columns.tolist():

        f = 'raw-{0}.{1}'.format(source, ext)
        fname = os.path.join(output_d, f)
        view = df_mean[source].sort(ascending=False, inplace=False)
        plotter(view[view != 0], fname, 'MCC', args)
        
        print(source, df_mean[source].mean(), df_mean[source].std())
        
        for target in df_mean.columns.tolist():
            if source == target:
                continue
            f = 'diff.{0}-{1}.{2}'.format(source, target, ext)
            fname = os.path.join(output_d, f)
            view = df_mean[source] - df_mean[target]
            view.sort(ascending=False)

            (_, p) = stats.ttest_ind(df_mean[source], df_mean[target])
            print(source, target, view.mean(), view.std(), p < 0.05)
            print(*distribution(view))
        
            plotter(view, fname, 'Difference in MCC', args)

# http://gestaltrevision.be/wiki/python/simplestats
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
            
            k = len(cf)         # within Group degrees of freedom
            N = k * len(groups) # between Group degrees of freedom

            msg = '{0:2d} {1:2d} '.format(i, len(groups))
            fmt = 'F({2}, {3}) = {0:6.3f}, p = {1:.3f} {4} '
            for j in (stats.f_oneway, stats.kruskal):
                (v, p) = j(*samples)
                msg += fmt.format(v, p, k - 1, N - k, issig(p))
            print(msg)

user = cli.CommandLine(cli.optsfile('prediction-plot'))
if not user.args.gfilter:
    user.args.gfilter = []

raw = pd.DataFrame.from_csv(user.args.data, sep=';', index_col=None)
assert(all([ x in raw.columns for x in user.args.gfilter]))
raw = raw.loc[raw['confusion_matrix'] != np.nan]

grouped = raw.groupby(user.args.gfilter + ['node'])['matthews_corrcoef']
df = grouped.agg([ np.mean, stats.sem ]).unstack(0)

if user.args.gfilter:
    args = {
        'figsize': (7, 3),
        'fontsize': 10,
        'kind': 'bar',
        'yerr': df['sem'],
        'ylim': (0, 1),
    }
    comparison(df, args, user.args.plotdir)
    performance(df, args, user.args.plotdir)

# anova(df['mean'], user.args.clusters)
