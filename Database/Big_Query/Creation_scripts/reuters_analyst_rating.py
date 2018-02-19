import pandas as pd
import datetime as DT
from google.cloud import bigquery


client = bigquery.Client()

headers = ['1-5 Linear Scale','(1) BUY','(2) OUTPERFORM','(3) HOLD','(4) UNDERPERFORM','(5) SELL','No Opinion','NaN','Mean Rating','constituent']
df_res = pd.DataFrame(columns = headers)
#print(df_res)
constituent_name = ['Adidas' ,'Allianz','BASF','Bayer','BMW','Beiersdorf AG','Commerzbank','Continental','Daimler','Deutsche Bank','Deutsche Boerse',
'Deutsche Lufthansa','Deutsche Post','Deutsche Telekom','E.ON','Fresenius Medical','Fresenius SE','HeidelbergCement','Henkel','Infineon','Linde','Merck','Munich Re Group',
'Prosiebensat1','RWE','SAP','Siemens','ThyssenKrupp','Volkswagen','Vonovia']
for constituent in constituent_name:
    if constituent == 'Allianz':
        name = 'ALVG.DE'
    elif constituent == 'Adidas':
        name = 'ADSGn.DE'
	elif constituent == 'BASF':
		name = 'BASFn.DE'
	elif constituent = 'Bayer'
		name = 'BAYGn.DE'
	elif constituent = 'BMW'
		 name = 'BMWG.DE'
	elif constituent = 'Beiersdorf AG'
		name = 'BEIG.DE'
	elif constituent = 'Commerzbank'
		name = 'CBKG.DE'
	elif constituent = 'Continental'
		name = 'CONG.DE'
	elif constituent = 'Daimler'
		name = 'DAIGn.DE'
	elif constituent = 'Deutsche Bank'
		name = 'DBKGn.DE'
	elif constituent = 'Deutsche Boerse'
		name = 'DB1Gn.DE'
	elif constituent = 'Deutsche Lufthansa'
		name = 'LHAG.DE'
	elif constituent = 'Deutsche Post'
		name = 'DPWGn.DE'
	elif constituent = 'Deutsche Telekom'
		name = 'DTEGn.DE'
	elif constituent = 'E.ON'
		name = 'EONGn.DE'
	elif constituent = 'Fresenius Medical'
		name = 'FMEG.DE'
	elif constituent = 'Fresenius SE'
		name = 'FREG.DE'
	elif constituent = 'HeidelbergCement'
		name = 'HEIG.DE'
	elif constituent = 'Henkel'
		name = 'HNKG_p.DE'
	elif constituent = 'Infineon'
		name = 'IFXGn.DE'
	elif constituent = 'Linde'
		name = 'LIN1.DE'
	elif constituent = 'Merck'
		name = 'MRCG.DE'
	elif constituent = 'Munich Re Group'
		name = 'MUVGn.DE'
	elif constituent = 'Prosiebensat1'
		name = 'PSMGn.DE'
	elif constituent = 'RWE'
		name = 'RWEG.DE'
	elif constituent = 'SAP'
		name = 'SAPG.DE'
	elif constituent = 'Siemens'
		name = 'SIEGn.DE'
	elif constituent = 'ThyssenKrupp'
		name = 'TKAG.DE'
	elif constituent = 'Volkswagen'
		name = 'VOWG_p.DE'
	elif constituent = 'Vonovia'
		name = 'VNAn.DE'		
	
		
	dfs = pd.read_html('https://www.reuters.com/finance/stocks/analyst/{}'.format(name))
    table = dfs[1]
    
    df1 = table[[0,1]]
    table = df1.transpose()
    table.columns = table.iloc[0]
    table1 = table.iloc[1:]
#table.reindex(table.index.drop(1))
    table1['constituent'] = constituent
    table1['date'] = "2018-02-19"
    df_res = df_res.append(table1)
#print(df_res)
buy =[]
buy = list(df_res.iloc[:,1])
outperform = []
outperform = list(df_res.iloc[:,3])
hold = []
hold = list(df_res.iloc[:,5])
underperform = []
underperform = list(df_res.iloc[:,7])
sell = []
sell = list(df_res.iloc[:,9])
mean_rating =[]
mean_rating = list(df_res.iloc[:,11])
constituent = []
constituent = list(df_res.iloc[:,14])
date = []
date = list(df_res.iloc[:,16])
#print(buy)
#print(outperform)
#print(hold)
#print(underperform)
#print(sell)
#print(mean_rating)
#print(constituent)
#print(date)
#print(buy[0])

for i in range(0,len(constituent)):
	print('----')
	print(buy[i])	
	query_insert = client.query("""INSERT INTO `igenie-project.pecten_dataset.reuters_analyst_rating` 
	(buy, outperform, hold, underperfrom , sell, mean_rating, constituent, date) 
	VALUES({},{},{},{},{},{},'{}','{}')""".format(buy[i],outperform[i],hold[i],underperform[i],sell[i],mean_rating[i],constituent[i],date[i]))
	insert_result = query_insert.result()
	