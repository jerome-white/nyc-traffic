import pandas as pd
import lib.node as nd
import lib.cpoint as cp
import lib.window as win
import rollingtools as rt

from lib import ngen
from lib import logger
from lib import window

#############################################################################

def mkrange(config, key, inclusive=True):
    vals = [ int(x) for x in config[key].split(',') ]
    if inclusive:
        vals[-1] += 1

    return vals

def f(args):
    (index, nid, (config, )) = args
    log = logger.getlogger()
    
    log.info('{0} setup'.format(nid))
    args = rt.mkargs(config)

    log.info('{0} apply'.format(nid))
    
    df = args.roller.apply(rt.apply, args=[ args.window, args.classifier ])
    assert(type(df) == pd.Series)
    df.rename('jam', inplace=True)
    
    log.info('{0} finished'.format(nid))
    
    return (nid, df)

#############################################################################

engine = ProcessingEngine('prediction')
log = logger.getlogger()

windows = engine.config['window']
keys = [ 'observation', 'prediction' ]
(observation_, prediction_) = [ mkrange(windows, x) for x in keys ]

for observation in range(*observation_):
    for i in [ 'observation', 'target' ]:
        windows[i] = observation
        
    for prediction in range(*prediction_):
        windows['prediction'] = prediction
        log.info(engine.config['window'])

        for (nid, df) in engine.run(f, ngen.SequentialGenerator()):
            # add in columns pertaining to node, observation and prediction
            df['node'] = nid
            for i in keys:
                df[i] = windows[i]

            # write DataFrame to database
            with db.DatabaseConnection() as connection:
                df.to_sql('occurrence', connection, index_label='as_of')
