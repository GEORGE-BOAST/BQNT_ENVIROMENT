import seaborn as sb
import pandas as pd
import matplotlib.pyplot as plt
import bql

bq = bql.Service()
query = """get(px_last)for('AAPL US EQUITY')with(dates=range(-1y,0d),fill='prev')"""
data = bql.combined_df(bq.execute(query)).reset_index()

fig = plt.figure(figsize=(12,8))
sb.lineplot(data=data, x='DATE',y='px_last')
plt.show()