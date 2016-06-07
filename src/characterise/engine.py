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
        log.info('phase 1')
        
        # /etc/opts/prediction
        cargs = cli.CommandLine(cli.optsfile(opts))
        self.log.info('configure ' + cargs.args.config)

        self.config = ConfigParser()
        self.config.read(cargs.args.config) # --config

        # Establish the database credentials. Passing None uses the
        # defaults.
        dbinfo = self.config['database'] if 'database' in self.config else None
        db.EstablishCredentials(**dbinfo)

        if init_db:
            reporting = float(self.config['parameters']['intra-reporting'])
            db.genop(reporting)

        self.log.info('db version: {0}'.format(db.mark()))

    def run(self, mapper, generator, parallel=True):
        '''
        mapper: should return a node id, pandas data frame pair
        generator: should be of type NodeGenerator
        '''
        
        self.log.info('processing')
        if not parallel:
            assert('node' in self.config['parameters'])
            
            seq = (0, int(self.config['parameters']['node']))
            return f(seq, self.config)
                     
        with Pool() as pool:
            f = mapper
            g = generator.nodegen
            yield from pool.imap_unordered(f, g(self.config), 1)
                
    def dump(self, data, fname=None):
        if not fname:
            pth = Path(self.args.config)
            fname = str(pth.with_suffix('.pkl'))

        data.to_pickle(fname)
