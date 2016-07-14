import csv
import shutil

from pathlib import Path

csv_file = '/Users/jerome/nyc/src/cluster/data/speed/dat.csv'
source = '/Users/jerome/nyc/src/characterise/log/speed'
target = '/Users/jerome/nyc/src/cluster/data/speed/plots'

index = 'nid'

tpath = Path(target)
shutil.rmtree(str(tpath), ignore_errors=True)

with open(csv_file) as fp:
    reader = csv.DictReader(fp)
    for row in reader:
        nid = row[index].zfill(3)
        png = Path(source, nid).with_suffix('.png')
        
        for i in filter(lambda x: x != index, row.keys()):
            group = 'group-' + i.zfill(2)
            cluster = 'cluster-' + row[i].zfill(2)
            
            dest = tpath.joinpath(group, cluster)
            dest.mkdir(parents=True, exist_ok=True)
            dest.joinpath(png.name).symlink_to(png)
