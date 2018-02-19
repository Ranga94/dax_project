import pandas as pd
import datetime as DT
from google.cloud import bigquery


client = bigquery.Client()

headers = ['1-5 Linear Scale','(1) BUY','(2) OUTPERFORM','(3) HOLD','(4) UNDERPERFORM','(5) SELL','No Opinion','NaN','Mean Rating','constituent']
df_res = pd.DataFrame(columns = headers)
#print(df_res)
constituent_name = ['adidas','allianz',]
for constituent in constituent_name:
    if constituent == 'allianz':
        name = 'ALVG.DE'
    elif constituent == 'adidas':
        name = 'ADSGn.DE'
    dfs = pd.read_html('https://www.reuters.com/finance/stocks/analyst/{}'.format(name))
    table = dfs[1]
    
    df1 = table[[0,1]]
    table = df1.transpose()
    table.columns = table.iloc[0]
    table1 = table.iloc[1:]
#table.reindex(table.index.drop(1))
  #  table1['constituent'] = constituent
 #   table1['date'] = "2018-02-19"
    df_res = df_res.append(table1)
#print(df_res)
buy = df_res.iloc[:,1]
outperform = df_res.iloc[:,3]
hold = df_res.iloc[:,5]
underperform = df_res.iloc[:,7]
sell = df_res.iloc[:,9]
mean_rating = df_res.iloc[:,11]
constituent = df_res.iloc[:,14]
#date = df_res.iloc[:,16]
#print(buy)
#print(outperform)
#print(hold)
#print(underperform)
#print(sell)
#print(mean_rating)
#print(constituent)
#print(date)

for i in range(0,len(constituent)):
	query_insert = client.query("""INSERT INTO `igenie-project.pecten_dataset.reuters_analyst_rating` (buy, outperform, hold, underperfrom , sell, mean_rating, constituent, date) VALUES({},{},{},{},{},{},'{}','{}'""".format(buy[i],outperform[i],hold[i],underperform[i],sell[i],mean_rating[i],constituent[i],date[i]))
	insert_result = query_insert.result()
	