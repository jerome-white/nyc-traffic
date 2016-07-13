from lib import db
from lib import cli
from lib import ngen
from lib import logger
from pathlib import Path
from collections import namedtuple
from configparser import ConfigParser
from multiprocessing import Pool
        
class ProcessingEngine:
    def __init__(self, opts, init_db=False):
        self.log = logger.getlogger(True)
        self.log.info('phase 1')
        
        # /etc/opts/prediction
        cargs = cli.CommandLine(cli.optsfile(opts))
        self.ini = cargs.args.config
        self.log.info('configure ' + self.ini)

        self.config = ConfigParser()
        self.config.read(self.ini) # --config

        # Establish the database credentials. Passing None uses the
        # defaults.
        dbinfo = self.config['database'] if 'database' in self.config else None
        db.EstablishCredentials(**dbinfo)

        if init_db:
            reporting = float(self.config['parameters']['intra-reporting'])
            db.genop(reporting)

        # self.log.info('db version: {0}'.format(db.mark()))

    def run(self, f, generator):
        '''
        f: should return a node id, pandas data frame pair
        generator: should be of type NodeGenerator
        '''

        if 'node' in self.config['parameters']:
            nodes = self.config['parameters']['node'].split(',')
            for i in enumerate(map(int, nodes)):
                yield f(i, self.config)
        else:
            with Pool() as pool:
                g = generator.nodegen
                yield from pool.imap_unordered(f, g(self.config), 1)
                
    def dump(self, data, fname=None):
        if not fname:
            pth = Path(self.ini)
            fname = str(pth.with_suffix('.pkl'))

        data.to_pickle(fname)

    def store(self, data, table, index='as_of'):
        with db.DatabaseConnection() as connection:
            data.to_sql(table, connection, index_label=index)
