import pandas as pd
from google.cloud import bigquery

Lufthansa_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-lufthansa-lha-ff/stock-research.html')
Lufthansa = Lufthansa_tables[5]
Lufthansa['constituent']= 'Lufthansa'

BMW_tables = pd.read_html('https://www.obermatt.com/en/stocks/bmw-bmw-ff/stock-research.html')
BMW = BMW_tables[5]
BMW['constituent'] = 'BMW'

Vonovia_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-annington-immobilien-se-ann-ff/stock-research.html')
Vonovia = Vonovia_tables[5]
Vonovia['constituent'] = 'Vonovia'

Daimler_tables = pd.read_html('https://www.obermatt.com/en/stocks/daimler-dai-ff/stock-research.html')
Daimler = Daimler_tables[5]
Daimler['constituent']= 'Daimler'

Bayer_tables = pd.read_html('https://www.obermatt.com/en/stocks/daimler-dai-ff/stock-research.html')
Bayer = Bayer_tables[5]
Bayer['constituent'] = 'Bayer'

Heidelberg_tables = pd.read_html('https://www.obermatt.com/en/stocks/heidelbergcement-hei-ff/stock-research.html')
Heidelberg = Heidelberg_tables[5]
Heidelberg['constituent'] = 'Heidelberg'

Fresenius_Medical_Care_tables = pd.read_html('https://www.obermatt.com/en/stocks/fresenius-medical-care-fme-ff/stock-research.html')
Fresenius_Medical_Care = Fresenius_Medical_Care_tables[5]
Fresenius_Medical_Care['constituent'] = 'Fresenius_Medical_Care'

BASF_tables = pd.read_html('https://www.obermatt.com/en/stocks/basf-bas-ff/stock-research.html')
BASF = BASF_tables[5]
BASF['constituent'] = 'BASF'

Fresenius_tables = pd.read_html('https://www.obermatt.com/en/stocks/fresenius-fre-ff/stock-research.html')
Fresenius = Fresenius_tables[5]
Fresenius['constituent'] = 'Fresenius'

Volkswagen_tables = pd.read_html('https://www.obermatt.com/en/stocks/volkswagen-vow-ff/stock-research.html')
Volkswagen = Volkswagen_tables[5]
Volkswagen['constituent'] = 'Volkswagen'

Merck_tables = pd.read_html('https://www.obermatt.com/en/stocks/merck-kgaa-mrk-ff/stock-research.html')
Merck = Merck_tables[5]
Merck['constituent'] = 'Merck'

Adidas_tables = pd.read_html('https://www.obermatt.com/en/stocks/adidas-ads-ff/stock-research.html')
Adidas = Adidas_tables[5]
Adidas['constituent'] = 'Adidas'

Deutsche_Post_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-post-dpw-ff/stock-research.html')
Deutsche_Post = Deutsche_Post_tables[5]
Deutsche_Post['constituent'] = 'Deutsche_Post'

Siemens_tables = pd.read_html('https://www.obermatt.com/en/stocks/siemens-sie-ff/stock-research.html')
Siemens = Siemens_tables[5]
Siemens['constituent'] ='Siemens'

Deutsche_Telekom_tables = pd.read_html('https://www.obermatt.com/en/stocks/deutsche-telekom-dte-ff/stock-research.html')
Deutsche_Telekom = Deutsche_Post_tables[5]
Deutsche_Telekom['constituent'] = 'Deutsche_Telekom'

Sap_tables = pd.read_html('https://www.obermatt.com/en/stocks/sap-sap-ff/stock-research.html')
Sap = Sap_tables[5]
Sap['constituent'] = 'SAP'

Continental_tables = pd.read_html('https://www.obermatt.com/en/stocks/continental-con-ff/stock-research.html')
Continental = Continental_tables[5]
Continental['constituent'] = 'Continental'

EON_tables = pd.read_html('https://www.obermatt.com/en/stocks/e-on-eoan-ff/stock-research.html')
EON = EON_tables[5]
EON['constituent'] = 'EON'

Henkel_tables = pd.read_html('https://www.obermatt.com/en/stocks/henkel-hen-ff/stock-research.html')
Henkel = Henkel_tables[5]
Henkel['constituent'] = 'Henkel'

Thyssenkrupp_tables = pd.read_html('https://www.obermatt.com/en/stocks/thyssenkrupp-tka-ff/stock-research.html')
Thyssenkrupp = Thyssenkrupp_tables[5]
Thyssenkrupp['constituent'] = 'Thyssenkrupp'

frames = [Lufthansa,BMW,Vonovia,Daimler,Bayer,Heidelberg,Fresenius_Medical_Care,BASF,Fresenius,Volkswagen,Merck,Adidas,Deutsche_Post,Siemens,Deutsche_Telekom,Sap,Continental,EON,Henkel,Thyssenkrupp]
results = pd.concat(frames)

results.to_csv('E:\value_metrics.csv')