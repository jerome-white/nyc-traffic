import os
import logging
import platform

# Level    Numeric value
# CRITICAL 50
# ERROR    40
# WARNING  30
# INFO     20
# DEBUG    10
# NOTSET   0

class LogConfigure:
    logname = None # basename for clients
    
    def __new__(self):
        if not self.logname:
            # log level
            level = logging.DEBUG

            # message format
            msgfmt = [
                '%(levelname)s %(asctime)s',
                '%(name)s',
                '%(filename)s:%(lineno)d',
                '%(message)s',
            ]
            msgsep = ' '
            msgfmt = msgsep.join(msgfmt)

            # date format
            mdy = [ 'Y', 'm', 'd' ]
            hms = [ 'H', 'M', 'S' ]
            datesep_intra = ''
            datesep_inter = ','

            mdyhms = [ [ '%' + x for x in y ] for y in [mdy, hms] ]
            datefmt = datesep_inter.join(map(datesep_intra.join, mdyhms))

            # configure!
            logging.basicConfig(level=level, format=msgfmt, datefmt=datefmt)
            self.logname = '.'.join(map(str, [ platform.node(), os.getpid() ]))
            
        return self.logname

def getlogger(root=False):
    elements = [ LogConfigure() ]
    if not root:
        elements.append(str(os.getpid()))
    name = '.'.join(elements)
    
    return logging.getLogger(name)
