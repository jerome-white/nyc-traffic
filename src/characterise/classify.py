import pandas as pd

from lib import ngen
from lib import logger
from lib import engine as eng
from lib import rollingtools as rt
from pathlib import Path

#############################################################################

def mkrange(config, key, inclusive=True):
    vals = [ int(x) for x in config[key].split(',') ]
    if inclusive:
        vals[-1] += 1

    return vals

def f(args):
    (index, nid, (config, )) = args
    log = logger.getlogger()
    log.info('+ {0}'.format(nid))

    args = rt.mkargs(nid, config)
    df = args.roller.apply(rt.apply, args=[ args.window, args.classifier ])
    df.rename(nid, inplace=True)

    return (nid, df)

#############################################################################

engine = eng.ProcessingEngine('prediction')
log = logger.getlogger()

root = Path(engine.config['output']['root'])

windows = engine.config['window']
keys = [ 'observation', 'prediction' ]
(observation, prediction) = [ mkrange(windows, x) for x in keys ]

for o in range(*observation):
    for i in [ 'observation', 'target' ]:
        windows[i] = '{0:02d}'.format(o)

    for p in range(*prediction):
        log.info('o: {0} p: {1}'.format(o, p))
        
        windows['prediction'] = '{0:02d}'.format(p)
        path = Path(root, windows['observation'], windows['prediction'])
        path.mkdir(parents=True, exist_ok=True)
        
        for (nid, df) in engine.run(f, ngen.SequentialGenerator()):
            log.info('- {0}'.format(nid))

            n = '{0:03d}'.format(nid)
            fname = path.joinpath(n).with_suffix('.pkl')
            engine.dump(df, str(fname))
