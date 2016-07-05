import uuid
import pathlib
import itertools

import pandas as pd

from lib import cli

args = cli.CommandLine(cli.optsfile('storage')).args

for i in itertools.count():
    try:
        df = pd.DataFrame.from_csv(url, sep='\t', index_col=[ 'DataAsOf' ])
        df = df.ix[df.index.max()]
        break
    except URLError:
        pass
    
    if i > args.retries:
        log.error('Retries exceeded')
        exit

fname = pathlib.Path(args.output, str(uuid.uuid4()))
df.to_pickle(str(fname))
