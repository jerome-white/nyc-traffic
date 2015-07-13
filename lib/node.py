import pymysql

import numpy as np
import pandas as pd
import datetime as dt

from lib import db
from tempfile import NamedTemporaryFile
from lib.logger import Logger
from collections import namedtuple
from statsmodels.tsa import stattools as st
from numpy.linalg.linalg import LinAlgError

Element = namedtuple('Element', [ 'node', 'lag' ])
Window = namedtuple('Window', [ 'observation', 'prediction', 'target' ])

def getnodes(connection, restrict=True):
    segments = [
        'SELECT id FROM node',
        'WHERE segment IS NOT NULL' if restrict else '',
        'ORDER BY id ASC',
        # 'LIMIT 1'
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

def neighbors_(source, levels, conn, f, ntree=None):
    if not ntree:
        root = Element(source, 0)
        ntree = { source.nid: root }

    if levels > 0:
        for i in source.neighbors.difference(ntree.keys()):
            node = Node(i, conn)
            lag = f(source, node) + ntree[source.nid].lag
            node.align(source, True)
            if complete(node.readings):
                ntree[i] = Element(node, lag)
                n = neighbors_(node, levels - 1, conn, f, ntree) # recurse!
                ntree.update(n)
            else:
                with NamedTemporaryFile(mode='w', delete=False) as fp:
                    node.readings.to_csv(fp)
                    m = '{0}->{1} {2}'.format(source.nid, node.nid, fp.name)
                    Logger().log.debug(m)
                    
    return ntree
            
def neighbors(source, levels, conn, f=lambda x, y: 0):
    return neighbors_(source, levels, conn, f).values()

def nacount(data, col='speed'):
    return data[col].isnull().sum()

def complete(data):
    return data.size > 0 and nacount(data) == 0
    
class Node:
    def __init__(self, nid, connection=None, start=None, stop=None):
        self.nid = nid
        self.freq = 'T'

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
    
    # ###################################################################

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
        
    # ###################################################################

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
        data = self.readings.reindex(other.range())
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

class Cluster:
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
