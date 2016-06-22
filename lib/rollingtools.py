import numpy as np
import lib.node as nd
import lib.cpoint as cp
import lib.window as win

from collections import namedtuple

CharArgs = namedtuple('CharArgs', [ 'node', 'classifier', 'window', 'roller' ])

def mkargs(nid, config):
    threshold = float(config['parameters']['acceleration'])    
    classifier = cp.Acceleration(threshold)
    
    window = win.from_config(config)

    node = nd.Node(nid)
    roller = node.readings.speed.rolling(len(window), center=True)

    return CharArgs(node, classifier, window, roller)

def apply(df, window, classifier):
    '''
    determine whether a window constitutes a traffic event
    '''
    assert(type(df) == np.ndarray)

    segments = (df[:window.observation], df[-window.target:])
    left_right = []
    
    for i in segments:
        if np.isnan(np.sum(i)):
            return np.nan
        left_right.append(i.mean())

    return classifier.classify(window.prediction + 1, *left_right)
