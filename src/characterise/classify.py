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

    log.info('{0} application'.format(nid))    
    df = args.roller.apply(rt.apply, args=[ args.window, args.classifier ])
    log.info('{0} finished'.format(nid))
    
    return (nid, df)

#############################################################################

engine = ProcessingEngine('prediction')
results = engine.run(f, ngen.SequentialGenerator())
panel = pd.Panel(dict(results))
engine.dump(panel)
