from lib import db

import flask
import random
from collections import namedtuple

app = flask.Flask(__name__)

MapSegment = namedtuple('MapSegment', [ 'id', 'coordinates', 'color' ])

def format_coordinates(segment):
    (x, y) = [ segment.find(x) for x in [ '(', ')' ] ]
    s = segment[x + 1:y]
    
    return [ [ float(y) for y in x.split(' ') ] for x in s.split(',') ]

@app.route('/')
def visualise():
    args = flask.request.args
    if 'api' not in args:
        return flask.render_template('error.html')
    
    segments = []
    used_colors = [ None ]
        
    with db.DatabaseConnection() as conn:
        with db.DatabaseCursor(conn) as cursor:
            sql = [ 'SELECT id, ST_ASTEXT(segment) AS segment',
                    'FROM node',
                    'WHERE segment IS NOT NULL',
                    ]
            cursor.execute(db.process(sql))
            for row in cursor:
                coords = format_coordinates(row['segment'])
                
                color = None
                while color in used_colors:
                    color = '#{0:06x}'.format(random.randint(0, 0xFFFFFF))
                used_colors.append(color)
                
                m = MapSegment(int(row['id']), coords, color)
                segments.append(m)

    return flask.render_template('m.html', api=args['api'], segments=segments)

app.run(host='0.0.0.0', debug=True)
