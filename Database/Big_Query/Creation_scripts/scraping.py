import pandas as pd
from google.cloud import bigquery

Lufthansa_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-lufthansa-lha-ff/stock-research.html')
Lufthansa = Lufthansa_tables[4]
Lufthansa['constituent']= 'Lufthansa'

BMW_tables = pd.read_html('https://www.obermatt.com/en/stocks/bmw-bmw-ff/stock-research.html')
BMW = BMW_tables[4]
BMW['constituent'] = 'BMW'

Vonovia_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-annington-immobilien-se-ann-ff/stock-research.html')
Vonovia = Vonovia_tables[4]
Vonovia['constituent'] = 'Vonovia'

Daimler_tables = pd.read_html('https://www.obermatt.com/en/stocks/daimler-dai-ff/stock-research.html')
Daimler = Daimler_tables[4]
Daimler['constituent']= 'Daimler'

Bayer_tables = pd.read_html('https://www.obermatt.com/en/stocks/daimler-dai-ff/stock-research.html')
Bayer = Bayer_tables[4]
Bayer['constituent'] = 'Bayer'

Heidelberg_tables = pd.read_html('https://www.obermatt.com/en/stocks/heidelbergcement-hei-ff/stock-research.html')
Heidelberg = Heidelberg_tables[4]
Heidelberg['constituent'] = 'Heidelberg'

Fresenius_Medical_Care_tables = pd.read_html('https://www.obermatt.com/en/stocks/fresenius-medical-care-fme-ff/stock-research.html')
Fresenius_Medical_Care = Fresenius_Medical_Care_tables[4]
Fresenius_Medical_Care['constituent'] = 'Fresenius_Medical_Care'

BASF_tables = pd.read_html('https://www.obermatt.com/en/stocks/basf-bas-ff/stock-research.html')
BASF = BASF_tables[4]
BASF['constituent'] = 'BASF'

Fresenius_tables = pd.read_html('https://www.obermatt.com/en/stocks/fresenius-fre-ff/stock-research.html')
Fresenius = Fresenius_tables[4]
Fresenius['constituent'] = 'Fresenius'

Volkswagen_tables = pd.read_html('https://www.obermatt.com/en/stocks/volkswagen-vow-ff/stock-research.html')
Volkswagen = Volkswagen_tables[4]
Volkswagen['constituent'] = 'Volkswagen'

Merck_tables = pd.read_html('https://www.obermatt.com/en/stocks/merck-kgaa-mrk-ff/stock-research.html')
Merck = Merck_tables[4]
Merck['constituent'] = 'Merck'

Adidas_tables = pd.read_html('https://www.obermatt.com/en/stocks/adidas-ads-ff/stock-research.html')
Adidas = Adidas_tables[4]
Adidas['constituent'] = 'Adidas'

Deutsche_Post_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-post-dpw-ff/stock-research.html')
Deutsche_Post = Deutsche_Post_tables[4]
Deutsche_Post['constituent'] = 'Deutsche_Post'

Siemens_tables = pd.read_html('https://www.obermatt.com/en/stocks/siemens-sie-ff/stock-research.html')
Siemens = Siemens_tables[4]
Siemens['constituent'] ='Siemens'

Deutsche_Telekom_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-telekom-dte-ff/stock-research.html')
Deutsche_Telekom = Deutsche_Post_tables[4]
Deutsche_Telekom['constituent'] = 'Deutsche_Telekom'

Sap_tables = pd.read_html('https://www.obermatt.com/en/stocks/sap-sap-ff/stock-research.html')
Sap = Sap_tables[4]
Sap['constituent'] = 'SAP'

Continental_tables = pd.read_html('https://www.obermatt.com/en/stocks/continental-con-ff/stock-research.html')
Continental = Continental_tables[4]
Continental['constituent'] = 'Continental'

EON_tables = pd.read_html('https://www.obermatt.com/en/stocks/e-on-eoan-ff/stock-research.html')
EON = EON_tables[4]
EON['constituent'] = 'EON'

Henkel_tables = pd.read_html('https://www.obermatt.com/en/stocks/henkel-hen-ff/stock-research.html')
Henkel = Henkel_tables[4]
Henkel['constituent'] = 'Henkel'

Thyssenkrupp_tables = pd.read_html('https://www.obermatt.com/en/stocks/thyssenkrupp-tka-ff/stock-research.html')
Thyssenkrupp = Thyssenkrupp_tables[4]
Thyssenkrupp['constituent'] = 'Thyssenkrupp'

frames = [Lufthansa,BMW,Vonovia,Daimler,Bayer,Heidelberg,Fresenius_Medical_Care,BASF,Fresenius,Volkswagen,Merck,Adidas,Deutsche_Post,Siemens,Deutsche_Telekom,Sap,Continental,EON,Henkel,Thyssenkrupp]
results = pd.concat(frames)

type = results.iloc[:,0]
year_2014 = results.iloc[:,1]
year_2015 = results.iloc[:,2]
year_2016 = results.iloc[:,3]
year_2017 = results.iloc[:,4]
constituent_name = results.iloc[:,5]
client = bigquery.Client()  

for i in range(0,len(results)):
	query_insert = client.query("""INSERT INTO `igenie-project.pecten_dataset_dev.obermatt`
		(`year_2014`, `year_2015`, `year_2016`, `year_2017`) 
		VALUES ({0}, {1}, {2}, {3},)""".format(year_2014[i], year_2015[i],year_2016[i], year_2017[i]))
	insert_result = query_insert.result()
	