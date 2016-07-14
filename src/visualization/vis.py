import flask
import random

import pandas as pd

from lib import db
from collections import namedtuple

app = flask.Flask(__name__)

MapSegment = namedtuple('MapSegment', [ 'id', 'coordinates', 'color' ])

class Color:
    def __init__(self):
        self.used = set([ None ])

    def random(self, hue=1):
        assert(0 <= hue <= 1)
        rng = round(0xFFFFFF * hue)
        
        return '#{0:06x}'.format(random.randrange(rng))

    def unique(self, hue=1):
        color = None
        while color in self.used:
            color = self.random(hue)
        self.used.add(color)

        return color

class Selection:
    def __init__(self):
        sql = [ 'SELECT id, ST_ASTEXT(segment) AS segment',
                'FROM node',
                'WHERE segment IS NOT NULL',
        ]
        self.sql = db.process(sql)

    def get_color(self, row):
        return 'red'
        
class Operational(Selection):
    def __init__(self):
        super().__init__()
        sql = [ 'SELECT n.id AS id, o.id AS valid,'
                'ST_ASTEXT(n.segment) AS segment',
                'FROM node n',
                'LEFT OUTER JOIN operational o ON n.id = o.id',
                'WHERE n.segment IS NOT NULL',
        ]
        self.sql = db.process(sql)

    def get_color(self, row):
        return 'black' if row['valid'] else 'red'

class MultiColor(Selection):
    def __init__(self):
        super().__init__()
        self.pallete = Color()

    def get_color(self, row):
        return self.pallete.unique()

class Cluster(Selection):
    def __init__(self, k=4):
        super().__init__()

        fname = '/Users/jerome/nyc/src/cluster/data/speed/dat.csv'
        self.df = pd.read_csv(fname, index_col='nid')
        self.df = self.df[k]
        
        color = Color()
        assert(all([ x in self.df.unique() for x in range(self.df.max()) ]))
        # self.pallete = [ color.unique() for _ in self.df.unique() ]
        self.pallete = [
            'red',
            'blue',
            'green',
            'orange',
            ]

        sql = [ 'SELECT id, ST_ASTEXT(segment) AS segment',
                'FROM node',
                'WHERE segment IS NOT NULL',
                'AND id IN ({0})'
        ]
        self.sql = db.process(sql, ','.join(map(str, self.df.index)))
        print(self.sql)

    def get_color(self, row):
        return self.pallete[self.df[row['id']]]
    
selection_ = {
    'default': Selection,
    'multicolor': MultiColor,
    'operational': Operational,
    'cluster': Cluster,
    }

def format_coordinates(segment):
    (x, y) = [ segment.find(x) for x in [ '(', ')' ] ]
    s = segment[x + 1:y]
    
    return [ [ float(y) for y in x.split(' ') ] for x in s.split(',') ]

@app.route('/')
def visualise():
    args = flask.request.args
    
    if 'api' not in args:
        return flask.render_template('error.html')

    if 'selection' in args:
        s = args['selection']
        if s == 'cluster' and 'k' in args:
            selection = selection_[s](args['k'])
        else:
            selection = selection_[s]()
    else:
        selection = Selection()

    segments = []
    with db.DatabaseConnection() as conn:
        with db.DatabaseCursor(conn) as cursor:
            cursor.execute(selection.sql)
            for row in cursor:
                coords = format_coordinates(row['segment'])
                color = selection.get_color(row)
                m = MapSegment(int(row['id']), coords, color)
                segments.append(m)

    return flask.render_template('m.html', api=args['api'], segments=segments)

app.run(host='0.0.0.0', debug=True)
