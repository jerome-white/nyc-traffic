import pandas as pd

import matplotlib
matplotlib.style.use('ggplot')
# import matplotlib.pyplot as plt
# plt.style.use('ggplot')

from lib import db
from lib import utils

with db.DatabaseConnection() as con:
    sql = [
        'SELECT node, frequency',
        'FROM quality'
    ]
    data = pd.read_sql_query(db.process(sql), con=con, index_col='node')

plot = data.hist(cumulative=True, normed=1, bins=len(data))
utils.mkplot_(plot, 'reporting.png')
