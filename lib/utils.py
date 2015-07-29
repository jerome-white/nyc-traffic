import os

import matplotlib.pyplot as plt

def mkplot_(plot, fname):
    plot.get_figure().savefig(fname, bbox_inches='tight')
    plt.close('all')

def mkplot(frame, fname, heading, ynorm=False, **kwargs):
    opts = {
        'grid': True,
        'kind': 'line',
        'legend': False,
        'linewidth': 0.5,
        # 'style': 'o', # http://tinyurl.com/mnk9gkw
        'title': heading,
        }
    if ynorm:
        opts['ylim'] = (0, 1)
    if kwargs:
        opts.update(kwargs)

    mkplot_(frame.plot(**opts), fname)
    
def mkfname(path, nid, extension='pdf'):
    return os.path.join(path, '{0:03d}.{1}'.format(nid, extension))
    
def mktitle(elements):
    (fmtstr, fmtlst) = ([], [])
    for (i, j) in enumerate(elements):
        (x, y) = j
        fmtstr.append(''.join(map(str, [ '{', i, x ])))
        fmtlst.append(y)

    return ' '.join(fmtstr).format(*fmtlst)
