import os
import pickle

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import statsmodels.tsa.vector_ar.var_model as vm

from os.path import expanduser
from tempfile import NamedTemporaryFile
from collections import namedtuple
from numpy.linalg import LinAlgError
from multiprocessing import Pool

from lib import db
from lib import cli
from lib.node import getnodes
from lib.logger import log
from lib.csvwriter import CSVWriter

def mkplot(node, kind, model, path, lags=0):
    path_ = os.path.join(path, kind)
    os.makedirs(path_, exist_ok=True)

    fname = '.'.join(map(str, [ node, 'irf', 'pdf' ]))
    fname = os.path.join(path_, fname)
            
    irf = model.irf(lags)

    plt.ioff()
    irf.plot()
    plt.savefig(fname)
    plt.close()
    
def ols_(*args):
    (_, nid, cargs) = args
    log.info('ols: {0}'.format(nid))

    node = nd.Cluster(nid)
    for i in cargs.lags:
        node.addlag(i)
        
    idx = repr(node)
    endog = node.readings[idx]
    exog = node.readings.drop(idx, axis=1)

    try:
        res = sm.OLS(endog=endog, exog=exog, missing='drop').fit()
        mkplot(node, 'ols', res, cargs.output)
    except (LinAlgError, ValueError) as err:
        log.error('{0}: {1},{2}'.format(err, endog.shape, exog.shape))

def var_(*args):
    (_, nid, cargs) = args
    
    node = nd.Cluster(nid)
    log.info('var: {0}'.format(str(node)))
    endog = node.readings.dropna()
    if not endog.empty and cargs.lags:
        maxlags = max(cargs.lags)
        try:
            res = vm.VAR(endog=endog).fit(maxlags=maxlags)
            mkplot(node, 'var', res, cargs.output, maxlags)
        except (LinAlgError, ValueError) as err:
            log.error(err)

# Fit = namedtuple('Fit', [ 'node', 'model', 'lags' ])
with Pool() as pool:
    cargs = cli.CommandLine(cli.optsfile('regression'))

    for i in [ var_, ols_ ]:        
        results = pool.starmap(i, nd.nodegen(cargs.args))
        # fname = os.path.join(cargs.args.output, i.__name__, '.pkl')
        # with open(fname, mode='wb') as fp:
        #     r = list(filter(None, results))
        #     pickle.dump(r, fp)
