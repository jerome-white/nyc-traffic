import pickle

import pandas as pd
import seaborn as sns

from lib import cli

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args

#
# Collect the data
#

with open(args.pickle, mode='rb') as fp:
    observations = pickle.load(fp)

average = observations.mean(axis=0)
variance = observations.std(axis=0)

#
# Plot
#

PlotInfo = namedtuple('PlotInfo', [ 'name', 'figure', 'labels' ])
plots = [
    PlotInfo('pwin-twin',
             sns.heatmap(average.iloc[::-1], annot=True, fmt='.0f'),
             { 'xlabel': 'Prediction window (minutes)',
               'ylabel': 'Adjacent windows (minutes)',
             }
         ),
    PlotInfo('variance',
             pd.DataFrame(variance.mean(axis=1)).plot(legend=None),
             { 'xlabel': 'Adjacent window (minutes)',
               'ylabel': 'Average standard deviation (minutes)',
             }
         ),
]

for i in plots:
    i.figure.set(**i.labels)
    f = Path(args.figures, i.name).with_suffix('.png')
    utils.mkplot_(i.figure, str(f))
