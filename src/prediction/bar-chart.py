import pathlib
import collections

import numpy as np
import pandas as pd

from lib import utils

import matplotlib
matplotlib.style.use('ggplot')

Params = collections.namedtuple('Params', ['directory', 'slices'])

d = collections.OrderedDict({
    'implementation': 'RandomForestClassifier',
})
p = Params('2015_11-13_1210.samjam.local', d)

# d = collections.OrderedDict({
#     'implementation': 'RandomForestClassifier',
#     'prediction': 2,
# })
# p = Params('2015_11-15_0943.samjam.local', d)

pivot = 'depth'
groups = list(p.slices.keys()) + [ pivot, 'node' ]
metrics = collections.OrderedDict({
    'f1_score': 'F$_{1}$',
    'matthews_corrcoef': 'MCC',
})

path = pathlib.PurePath('log', p.directory)

dat = pathlib.Path(path, 'dat')
raw = pd.DataFrame.from_csv(str(dat), sep=';', index_col=None)

grouped = raw.groupby(groups)[list(metrics.keys())]

aggregate = grouped.agg([ np.mean ])
df = aggregate.loc[list(p.slices.values())]
df = df.unstack(level=pivot)
df.rename(columns=lambda x: metrics[x] if x in metrics else x, inplace=True)

operations = [ df.mean, df.sem ]
(means, errors) = [ f().unstack(level=0).loc['mean'] for f in operations ]
fig = means.plot(yerr=errors, kind='bar')

fname = 'neighbor-' + '_'.join(map(str, p.slices.values()))
dat = pathlib.Path(path, 'fig', fname).with_suffix('.' + 'png')
utils.mkplot_(fig, str(dat))
