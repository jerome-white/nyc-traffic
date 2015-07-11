import db
import os
import pickle

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import statsmodels.tsa.vector_ar.var_model as vm

from cli import CommandLine
from node import getnodes
from logger import log
from os.path import expanduser
from tempfile import NamedTemporaryFile
from csvwriter import CSVWriter
from collections import namedtuple
from numpy.linalg import LinAlgError
from multiprocessing import Pool

class Node:
    def __init__(self, nid):
        self.nid = nid
        with db.DatabaseConnection() as connection:
            self.neighbors = self.__get_neighbors(connection)
            self.readings = self.__get_readings(self.neighbors, connection)

    def addlag(self, lag, inclusive=False, delimiter='-'):
        cols = list(self.neighbors)
        if inclusive:
            cols += [ self.nid ]

        for i in map(str, cols):
            column = delimiter.join([ i, str(lag) ])
            self.readings[column] = self.readings[i].shift(lag)
        
    def __repr__(self):
        return str(self.nid)

    def __str__(self):
        return '{0:03d}'.format(self.nid)

    def __where_clause(self, neighbors, splt=3):
        a = ' node = '.join(map(str, [''] + neighbors)).split()
        b = [ a[x:x + splt] for x in range(0, len(a), splt) ]
        
        return ' or '.join([ ' '.join(x) for x in b ])
        
    def __get_readings(self, neighbors, connection):
        nodes = list(neighbors) + [ self.nid ]
        sql = ('SELECT as_of, node, speed ' +
               'FROM reading ' +
               'WHERE {0}')
        sql = sql.format(self.__where_clause(nodes))
        
        data = pd.read_sql_query(sql, con=connection)
        data.reset_index(inplace=True)
        data = data.pivot(index='as_of', columns='node', values='speed')
        data.columns = data.columns.astype(str)
        
        return data.resample('T')

    def __get_neighbors(self, connection):
        sql = ('SELECT target.id AS id ' +
               'FROM node source, node target ' +
               'WHERE INTERSECTS(source.segment, target.segment) ' +
               'AND source.id = {0} AND target.id <> {0}')
        sql = sql.format(self.nid)
        with db.DatabaseCursor(connection) as cursor:
            cursor.execute(sql)
            return frozenset([ row['id'] for row in cursor ])

def stargen(cli):
    with db.DatabaseConnection() as conn:
        for (i, j) in enumerate(getnodes(conn)):
            yield (i, j, cli.args)

def mkplot(node, kind, model, path, lags=0):
    path_ = os.path.join(path, kind)
    os.makedirs(path_, exist_ok=True)

    fname = '.'.join(map(str, [ node, 'irf', 'png' ]))
    fname = os.path.join(path_, fname)
            
    irf = model.irf(lags)
    irf.plot()
    
    plt.savefig(fname)
    plt.close()
    
def ols_(*args):
    (_, nid, cli) = args
    log.info('ols: {0}'.format(nid))

    node = Node(nid)
    for i in cli.lags:
        node.addlag(i)
        
    idx = repr(node)
    endog = node.readings[idx]
    exog = node.readings.drop(idx, axis=1)

    try:
        res = sm.OLS(endog=endog, exog=exog, missing='drop').fit()
        mkplot(node, 'ols', res, cli.output)
    except (LinAlgError, ValueError) as err:
        log.error('{0}: {1},{2}'.format(err, endog.shape, exog.shape))

def var_(*args):
    (_, nid, cli) = args
    
    node = Node(nid)
    log.info('var: {0}'.format(str(node)))
    endog = node.readings.dropna()
    if not endog.empty and cli.lags:
        maxlags = max(cli.lags)
        try:
            res = vm.VAR(endog=endog).fit(maxlags=maxlags)
            mkplot(node, 'var', res, cli.output, maxlags)
        except (LinAlgError, ValueError) as err:
            log.error(err)

# Fit = namedtuple('Fit', [ 'node', 'model', 'lags' ])
with Pool() as pool:
    cli = CommandLine(expanduser('~/.trafficrc/opts.regression'))

    for i in [ var_, ols_ ]:        
        results = pool.starmap(i, stargen(cli))
        # fname = os.path.join(cli.args.output, i.__name__, '.pkl')
        # with open(fname, mode='wb') as fp:
        #     r = list(filter(None, results))
        #     pickle.dump(r, fp)
