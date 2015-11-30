import numpy as np
import pandas as pd
import scipy.constants as constant

# import matplotlib
# matplotlib.style.use('ggplot')
import matplotlib.pyplot as plt
plt.style.use('ggplot')

from lib import db
from lib import utils

with db.DatabaseConnection() as con:
    sql = [
        'SELECT frequency / {0} AS freq',
        'FROM quality',
        'ORDER BY frequency ASC',
    ]
    sql = db.process(sql, [ constant.minute ])
    df = pd.read_sql_query(sql, con=con)

df['dist'] = df.apply(lambda x: (x.index + 1) / len(df))

args = {
    'xlim': (1, 5),
    'ylim': (0, 1),
    'yticks': np.linspace(0, 1, 11),
    'legend': False
    }
plot = df.plot(x='freq', y='dist', **args)
plot.set_xlabel('Reporting frequency (min)')
plot.set_ylabel('Fraction of segments')

utils.mkplot_(plot, 'reporting.png')
