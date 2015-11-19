import random
import pathlib

import matplotlib.pyplot as plt

def mkplot_(plot, fname):
    fig = plot.get_figure()
    
    # plt.tight_layout()
    # fig.set_tight_layout({'pad': 1.08 * 2})
    fig.savefig(fname, bbox_inches='tight')
    
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
    fname = '{0:03d}'.format(nid)
    f = pathlib.Path(path, fname).with_suffix('.' + extension)
    
    return str(f)
    
def mktitle(elements):
    (fmtstr, fmtlst) = ([], [])
    for (i, j) in enumerate(elements):
        (x, y) = j
        fmtstr.append(''.join(map(str, [ '{', i, x ])))
        fmtlst.append(y)

    return ' '.join(fmtstr).format(*fmtlst)

def hexcolor(black=0, white=255):
    hx = [ '{0:02X}'.format(random.randint(black, white)) for _ in range(3) ]
    return '#' + ''.join(hx)
