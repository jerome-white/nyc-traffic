import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def pltvar(axis, xy, df, labels, stem):
    (x, y) = xy
    (xlabel, ylabel) = labels
    
    df = data.std(axis=axis)
    df.name = 'deviation'
    df = df.reset_index()
    
    kwargs = { 'x': x, 'y': y, 'data': df }
    sns.boxplot(palette="PRGn", whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)
    
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    
    dest = source.joinpath('variance-' + stem).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()


args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
source = Path(args.source)
xlabel = 'Adjacent windows (minutes)'

for i in map(str, source.glob('*.pkl')):
    data = pd.from_pickle(i)
    
    log.info('visualize: mean')

    df = data.mean(axis=1).unstack('observation')
    df = df.ix[df.index.sort_values(ascending=False)]
    sns.heatmap(df, annot=True, fmt='.0f')
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Prediction window (minutes)')
    dest = source.joinpath('frequency-' + i.stem).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

    log.info('visualize: variance')    

    kwargs = {
        'axis': 0,
        'xy': [ 'deviation', 'observation' ],
        'labels': [ xlabel, 'Observation window std. dev. (jams/day)' ],
        'stem': i.stem,
        }
    pltvar(**kwargs)
    
    kwargs['axis'] += 1
    kwargs['xy'].reverse()
    kwargs['labels'][1] = 'Prediction window std. dev. (jams/day)'

    pltvar(**kwargs)
