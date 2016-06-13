import pandas as pd
import lib.node as nd
import lib.cpoint as cp
import lib.window as win
import rollingtools as rt

from lib import ngen
from lib import logger
from lib import window

#############################################################################

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

for outer in range(1, 11):
    additional = { 'observation': outer }
    for inner in range(1, 11):
        additional['prediction'] = inner
        
        for (i, j) in zip(win.names, [ outer, inner, outer ]):
            engine.config['window'][i] = j
        log.info(engine.config['window'])

        for (nid, df) in engine.run(f, ngen.SequentialGenerator()):
            additional['node'] = nid
            for (i, j) in additional.items():
                df[i] = j
                
            with db.DatabaseConnection() as connection:
                results.to_sql('occurrence', connection, index_label='as_of')
