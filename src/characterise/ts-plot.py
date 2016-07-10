import pandas as pd
import matplotlib.pyplot as plt

from lib import ngen
from lib import logger
from pathlib import Path

from lib import node as nd
from lib import engine as eng

#############################################################################
    
def f(args):
    (index, nid, (config, )) = args
    logger.getlogger().info(nid)

    n = nd.Node(nid)
    grouped = n.readings.speed.groupby(lambda x: x.hour)
    product = [ pd.Series(x.values) for (_, x) in grouped ]
    df = pd.concat(product, axis=1, ignore_index=True)

    ax = df.boxplot(grid=False, sym=',', return_type='axes')
    ax.grid(b=True, axis='y')
    ax.set_xlabel('Hour of the day')
    ax.set_ylabel('Speed (mph)')
    plt.title(str(n))

    fname = '{0:03d}'.format(nid)
    path = Path(config['output']['destination'], fname).with_suffix('.png')
    plt.savefig(str(path))
    plt.close()

    return (nid, df)

#############################################################################

plt.style.use('ggplot')

log = logger.getlogger(True)
engine = eng.ProcessingEngine('prediction', init_db=False)#True)
destination = Path(engine.config['output']['destination'])
destination.mkdir(parents=True, exist_ok=True)
        
for (nid, _) in engine.run(f, ngen.SequentialGenerator()):
    pass
