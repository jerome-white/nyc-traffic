import pathlib
import collections

import numpy as np
import pandas as pd

from lib import utils

import matplotlib
matplotlib.style.use('ggplot')

Params = collections.namedtuple('Params', ['directory', 'slices'])

# d = collections.OrderedDict([
#     ('implementation', 'RandomForestClassifier'),
# ])
# p = Params('2015_11-13_1210.samjam.local', d)

d = collections.OrderedDict([
    ('implementation', 'RandomForestClassifier'),
#    ('prediction', 9),
])
p = Params('2015_11-15_0943.samjam.local', d)
pivot = [ 'prediction' ]

# d = collections.OrderedDict([
#     ('implementation', 'RandomForestClassifier'),
#     ('target', 8),
# ])
# p = Params('2015_11-20_2344.samjam.local', d)
pivot = [ 'target' ]

pivot.append('depth')
groups = list(p.slices.keys()) + pivot + [ 'node' ]
metrics = collections.OrderedDict({
    # 'f1_score': 'F$_{1}$',
    'matthews_corrcoef': 'MCC',
})

path = pathlib.PurePath('log', p.directory)
dat = pathlib.Path(path, 'dat')

raw = pd.DataFrame.from_csv(str(dat), sep=';', index_col=None)
grouped = raw.groupby(groups)[list(metrics.keys())]

df = grouped.agg([ np.mean ])
for i in p.slices.values():
    df = df.loc[i]
df = df.unstack(level=pivot)

stats = []
for i in (df.mean, df.sem):
    d = i()
    level = d.index.names.index(pivot[0])
    d = d.unstack(level=level)

    # level = len(metrics.keys()) + 1
    d.index = d.index.droplevel([0, 1])
    stats.append(d)
(means, errors) = stats

yticks = np.linspace(0, 1, 11)
fig = means.plot(kind='bar', yerr=errors, ylim=(0,1), yticks=yticks)

fname = 'neighbor-' + '_'.join(map(str, p.slices.values()))
dat = pathlib.Path(path, 'fig', fname).with_suffix('.' + 'png')
print(dat)

utils.mkplot_(fig, str(dat))
