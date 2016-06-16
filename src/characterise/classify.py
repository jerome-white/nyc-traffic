import pandas as pd

from lib import ngen
from lib import logger
from lib import engine
from lib import rollingtools as rt

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
    log.info('{0} finished'.format(nid))
    
    return (nid, df)

#############################################################################

engine = engine.ProcessingEngine('prediction')
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

        for (nid, data) in engine.run(f, ngen.SequentialGenerator()):
            assert(type(data) == pd.Series)
            df = pd.DataFrame({ 'jam': data,
                                'node': nid,
                                'observation': observation,
                                'prediction': prediction,
                                })

            # write DataFrame to database
            with db.DatabaseConnection() as connection:
                df.to_sql('occurrence', connection, index_label='as_of')
