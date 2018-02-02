import pandas as pd

lufthansa = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-lufthansa-lha-ff/stock-research.html')
for df in lufthansa:
	print(df)