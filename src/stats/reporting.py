import numpy as np
import pandas as pd
import lib.node as nd

from multiprocessing import Pool

from lib.logger import log
from lib.csvwriter import CSVWriter 

def f(*args):
    (_, nid, (seconds,)) = args
    
    log.info('node: {0}'.format(nid))

    n = nd.Node(nid, freq=None)
    times = pd.Series(n.readings.index)
    rtimes = times.diff().mean() / pd.Timedelta(seconds=seconds)

    return { 'node': nid, 'inter': rtimes }

with Pool() as pool:
    seconds = 1
    results = pool.starmap(f, nd.nodegen([ seconds ]), 1)

results = filter(lambda x: x['inter'] is not pd.NaT, results)
times = [ x['inter'] for x in results ]
# print(times, max(times), min(times), sep='\n')

for i in (max, min, np.nanmean, np.nanstd):
    log.info('{0}: {1:.2f}'.format(i.__name__, i(times)))

# header = results[0].keys()
# with CSVWriter(header) as writer:
#     writer.writeheader()
#     for i in results:
#         writer.writerows(i)
