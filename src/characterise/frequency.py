import pickle

import numpy as np
import pandas as pd
import seaborn as sns
import lib.cpoint as cp
import rollingtools as rt
import scipy.constants as constant
import matplotlib.pyplot as plt

from lib import db
from lib import cli
from lib import ngen
from lib import utils
from lib import logger
from lib import node as nd
from pathlib import Path
from lib.window import Window
from collections import namedtuple
from multiprocessing import Pool

#############################################################################

resample_ = 'D'
# resample_ = '3H'

def loop(window):
    for p in range(1, window.prediction + 1):
        for t in range(1, window.target + 1):
            yield Window(t, p, t)
    
def f(args):
    (index, nid, (config, )) = args
    log = logger.getlogger()
    
    log.info('{0} create'.format(nid))
    args = rt.mkargs(config)
    
    df = pd.DataFrame()
    for i in loop(window):
        log.info('{0} window {1}'.format(nid, i))
        frame = args.roller.apply(rt.apply, args=[ i, args.classifier ])
        aggregate = frame.resample(resample_).sum().mean()
        df.set_value(i.prediction, i.target, aggregate)
        
    return (nid, df)

#############################################################################

engine = ProcessingEngine('chgpt')
results = engine.run(f, ngen.SequentialGenerator())
panel = pd.Panel(dict(results))
engine.dump(panel)
