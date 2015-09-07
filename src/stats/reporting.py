import scipy.constants as constant

import numpy as np
import pandas as pd
import lib.node as nd
import statsmodels.api as sm
import matplotlib.pyplot as plt

from collections import namedtuple
from multiprocessing import Pool

from lib import utils
from lib.logger import log
from lib.csvwriter import CSVWriter 

Mapping = namedtuple('Mapping', [ 'node', 'inter' ])

def f(*args):
    (_, nid, (seconds,)) = args
    
    log.debug('node: {0}'.format(nid))
    
    n = nd.Node(nid, freq=None)
    times = pd.Series(n.readings.index)
    rtimes = times.diff().mean() / pd.Timedelta(seconds=seconds)

    return Mapping(nid, rtimes)

with Pool() as pool:
    seconds = 60
    results = pool.starmap(f, nd.nodegen([ seconds ]), 1)

results = filter(lambda x: x.inter is not pd.NaT, results)

xvals = []
with CSVWriter(Mapping._fields) as writer:
    writer.writeheader()
    for i in results:
        writer.writerow(i._asdict())
        xvals.append(float(i.inter))
xvals.sort()
yvals = [ (x + 1) / len(xvals) for (x, _) in enumerate(xvals) ]

for i in (max, min, np.nanmean, np.nanstd):
    log.info('{0}: {1:.2f}'.format(i.__name__, i(xvals)))

plt.xlim((0, 10))
plt.xlabel('Reporing time (min)')
plt.ylabel('Fraction of segments')
plt.plot(xvals, yvals)

plt.savefig('reporting-times.pdf', bbox_inches='tight')
plt.close('all')
