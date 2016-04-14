from lib import db
from lib import cli
from configparser import ConfigParser

cargs = cli.CommandLine(cli.optsfile('prediction')) # /etc/opts/prediction
args = cargs.args

config = ConfigParser()
config.read(args.config) # --config

dbinfo = config['database'] if 'database' in config else None
db.EstablishCredentials(**dbinfo)

db.genop(int(config['parameters']['intra-reporting']))
