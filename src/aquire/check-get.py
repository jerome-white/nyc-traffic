import pickle

from lib import db
from lib import cli
from collections import Counter

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

with open(args.input, mode='rb') as fp:
    data = pickle.load(fp)
    
keys = []
values = []
counts = Counter()
for i in data:
    if not keys:
        keys = i.keys()
    values.append([ i[x] for x in keys ])
    counts[i['node']] += 1

# print(len(values))
# for i in values:
#     print(i)

for i in counts:
    print(i, counts[i])
