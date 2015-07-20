import numpy as np
import pandas as pd
import datetime as dt

from tempfile import NamedTemporaryFile
from collections import namedtuple
from statsmodels.tsa import stattools as st

from lib import db
from lib import cluster as cl
from lib.logger import log

Element = namedtuple('Element', [ 'node', 'lag', 'root' ])
Window = namedtuple('Window', [ 'observation', 'prediction', 'target' ])

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

def nodegen(args):
    with db.DatabaseConnection() as conn:
        for (i, j) in enumerate(getnodes(conn)):
            yield (i, j, args)

def neighbors_(source, levels, cluster, conn, ntree=None):
    if not ntree:
        root = Element(source, 0, True)
        ntree = { source.nid: root }

    if levels > 0:
        try:
            cl = cluster(source.nid, conn)
        except AttributeError as err:
            log.error(err)
            return ntree

        for i in cl.neighbors:
            if i not in tree:
                try:
                    lag = cl.lag(i)
                except ValueError as err:
                    log.error(err)
                    continue

                if i in ntree:
                    element = ntree[i]
                    node = element.node
                    lag += element.lag
                else:
                    node = Node(i, conn)
                    
                ntree[i] = Element(node, lag, False)
                node = ntree[i].node
                n = neighbors_(node, levels - 1, cluster, conn, ntree)
                ntree.update(n)
                    
    return ntree
            
def neighbors(source, levels, cluster, conn):
    n = neighbors_(source, levels, cluster, conn)
    for (i, j) in n.items():
        if i != source.nid:
            j.node.align(source, True)

    return n.values()

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
        self.neighbors = self.__get_neighbors(connection)
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
                    
        sql = ('SELECT as_of, speed, travel_time / 60 AS travel ' +
               'FROM reading ' +
               'WHERE node = {0}{1} ' +
               'ORDER BY as_of ASC')
        sql = sql.format(self.nid, ' AND '.join(option))
        
        data = pd.read_sql_query(sql, con=connection, index_col='as_of')
        data.columns = [ 'speed', 'travel' ]
        
        return data.resample(self.freq)

    def __get_neighbors(self, connection):
        sql = ('SELECT target.id AS id ' +
               'FROM node source, node target ' +
               'WHERE INTERSECTS(source.segment, target.segment) ' +
               'AND source.id = {0} AND target.id <> {0}')
        sql = sql.format(self.nid)
        with db.DatabaseCursor(connection) as cursor:
            cursor.execute(sql)
            return frozenset([ row['id'] for row in cursor ])
        
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
