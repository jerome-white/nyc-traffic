import pandas as pd
import seaborn as sns

from lib import db
from lib import ngen
from lib import utils
from lib import logger
from pathlib import Path
from collections import namedtuple

#############################################################################

resample_ = 'D'
# resample_ = '3H'

def f(args):
    (index, nid, (config, )) = args
    log = logger.getlogger()

    log.info('{0} aquire'.format(nid))
    
    sql = [ 'SELECT as_of, jam, observation, prediction',
            'FROM occurrence',
            'WHERE node = {0}',
            ]
    sql = db.process(sql, nid)
    with db.DatabaseConnection() as con:
        data = pd.read_sql_query(sql, con=con, index_col='as_of')

    log.info('{0} process'.format(nid))
    
    levels = ('observation', 'prediction')
    df = data.groupby(*levels)
    df = df.resample(config['frequency']).sum()
    df = df['jam'].mean(level=levels).unstack()

    return (nid, df)

#############################################################################

log.info('collect')

engine = ProcessingEngine('chgpt')
results = engine.run(f, ngen.SequentialGenerator())
panel = pd.Panel(dict(results))

log.info('manipulate')

average = panel.mean(axis=0)
variance = panel.std(axis=0)

log.info('visualize')

PlotInfo = namedtuple('PlotInfo', [ 'name', 'figure', 'labels' ])
plots = [
    PlotInfo('pwin-twin',
             sns.heatmap(average.iloc[::-1], annot=True, fmt='.0f'),
             { 'xlabel': 'Prediction window (minutes)',
               'ylabel': 'Adjacent windows (minutes)',
             }
         ),
    PlotInfo('variance',
             pd.DataFrame(variance.mean(axis=1)).plot(legend=None),
             { 'xlabel': 'Adjacent window (minutes)',
               'ylabel': 'Average standard deviation (minutes)',
             }
         ),
]

for i in plots:
    i.figure.set(**i.labels)
    f = Path(args.figures, i.name).with_suffix('.png')
    utils.mkplot_(i.figure, str(f))
