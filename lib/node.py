import numpy as np
import pandas as pd
import datetime as dt
import scipy.constants as constant

from tempfile import NamedTemporaryFile
from collections import namedtuple
from statsmodels.tsa import stattools as st

from lib import db
from lib.logger import log

Element = namedtuple('Element', [ 'node', 'lag', 'root' ])
Window = namedtuple('Window', [ 'observation', 'prediction', 'target' ])

def winsum(window):
    return window.observation + window.prediction + window.target

def getnodes(connection, restrict=True):
    segments = [
        'SELECT id FROM node',
        'WHERE segment IS NOT NULL' if restrict else '',
        'ORDER BY id ASC',
    ]
    sql = ' '.join(segments)
    
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
        for row in cursor:
            yield row['id']
            
def nodegen(args=None):
    k = tuple(args) if type(args) == list else args
    
    with db.DatabaseConnection() as conn:
        for (i, j) in enumerate(getnodes(conn)):
            yield (i, j, k)

def get_neighbors(nid, connection, xfun='ST_INTERSECTS'):
    sql = ('SELECT target.id AS nid ' +
           'FROM node source, node target ' +
           'WHERE {1}(source.segment, target.segment) ' +
           'AND source.id = {0} AND target.id <> {0}')
    sql = sql.format(nid, xfun)
    
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
        return frozenset([ row['nid'] for row in cursor ])
    
def neighbors_(source, levels, cluster, conn, seen=None):
    if not seen:
        root = Element(source, 0, True)
        seen = { source.nid: root }

    if levels > 0:
        try:
            cl = cluster(source.nid, conn)
        except AttributeError as err:
            log.error(err)
            return seen
        
        existing_lag = seen[source.nid].lag
        for i in cl.neighbors.difference(seen.keys()):
            try:
                lag = cl.lag(i) + existing_lag
            except ValueError as err:
                log.error(err)
                continue

            node = Node(i, conn)
            seen[i] = Element(node, lag, False)
            n = neighbors_(node, levels - 1, cluster, conn, seen)
            seen.update(n)
                    
    return seen
            
def neighbors(source, levels, cluster, conn, align_and_shift=True):
    elements = neighbors_(source, levels, cluster, conn).values()
    
    msg = ', '.join(map(lambda x: ':'.join(map(repr, x)), elements))
    log.debug('neighbors: {0}'.format(msg))

    if align_and_shift:
        for i in elements:
            if not i.root:
                i.node.readings.shift(i.lag)
                i.node.align(source, True)

    return [ x.node for x in elements ]

def nacount(data, col='speed'):
    return data[col].isnull().sum()

def complete(data, col='speed'):
    return data.size > 0 and nacount(data, col) == 0
    
class Node:
    def __init__(self, nid, connection=None, start=None, stop=None, freq='T'):
        self.nid = nid
        self.freq = freq

        close = not connection
        if close:
            connection = db.DatabaseConnection().resource
            
        self.readings = self.__get_readings(connection, [ start, stop ])
        self.neighbors = get_neighbors(self.nid, connection)
        self.name = self.__get_name(connection)

        if close:
            connection.close()

    def __repr__(self):
        return str(self.nid)
    
    def __str__(self):
        return repr(self) + ': ' + self.name

    def __eq__(self, other):
        return self.nid == other.nid

    def __hash__(self):
        return self.nid
    
    def __get_name(self, connection):
        sql = ('SELECT name ' +
               'FROM node '+
               'WHERE id = {0}')
        sql = sql.format(self.nid)
        
        with db.DatabaseCursor(connection) as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()

        return row['name']
        
    def __get_readings(self, connection, drange):
        option = ['']
        if any(drange):
            for (i, j) in zip(drange, [ '>', '<' ]):
                if i:
                    tm = i.strftime('%Y-%m-%d %H:%M:%S')
                    option.append("as_of {1}= '{0}'".format(j, tm))
                    
        sql = ('SELECT as_of, speed, travel_time / {2} AS travel ' +
               'FROM reading ' +
               'WHERE node = {0}{1} ' +
               'ORDER BY as_of ASC')
        sql = sql.format(self.nid, ' AND '.join(option), constant.minute)
        
        data = pd.read_sql_query(sql, con=connection, index_col='as_of')
        data.columns = [ 'speed', 'travel' ]
        
        return data.resample(self.freq) if self.freq else data

    def range(self, window, bound=True):
        idx = self.readings.index
        
        for (i, _) in enumerate(idx):
            j = i + window.observation
            k = j + window.prediction
            l = k + window.target
            
            if bound and l > idx.size:
                raise StopIteration
            
            yield (idx[i:j], idx[k:l])
    
    def align(self, other, interpolate=False):
        data = self.readings.reindex(other.readings.index)
        if interpolate:
            # XXX think about limiting this!
            data = data.interpolate().fillna(method='backfill')

        self.readings = data

    def partition(self, pct):
        assert(0 <= pct <= 1)

        line = round(len(self.readings) * pct)

        return (self.readings.ix[:line], self.readings.ix[line:])

    def stationary(self, sig=0.05):
        (adf, pvalue, _, _, crit, *_) = st.adfuller(self.readings)

        return pvalue < sig and adf < max(crit.values())
    
    def purge(self, thresh):
        data = self.readings
        condition = np.abs(data - data.mean()) <= thresh * data.std()

        return self.readings[condition]
