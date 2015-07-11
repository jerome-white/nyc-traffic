import numpy as np

from logger import Logger
from sklearn.cross_validation import StratifiedShuffleSplit

def cleanse(data):
    assert(data)

    pruned = np.empty([])
    whole = np.asfarray(data)
            
    ratio = np.PINF
    finites = np.isfinite(whole)

    before = whole.shape
    for (i, j) in enumerate(before[::-1]):
        mask = finites.all(axis=i)
        rat = mask.sum() / j
        if 0 < rat < ratio:
            ratio = rat
            pruned = whole[mask] if i else whole[:,mask]

    for i in (pruned, whole):
        if i.size > 0:
            Logger().log.debug('{0}->{1}'.format(before, i.shape))
            return (i[:,:-1], i[:,-1:].ravel())
    
    raise AttributeError('Unable to prune')

def kfold(data, folds, testing=0.2):
    assert(0 < testing < 1)
    
    if not data:
        raise StopIteration

    (features, labels) = cleanse(data)
    try:
        strat = StratifiedShuffleSplit(labels, n_iter=folds, test_size=testing)
    except ValueError as err:
        Logger().log.error(err)
        raise StopIteration
    
    for (train, test) in strat:
        yield (features[train], features[test], labels[train], labels[test])
