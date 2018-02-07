import bs4 as BeautifulSoup
from selenium import webdriver
import pandas as pd

def get_tab(df, url):
    # getting javascript tables 
    driver = webdriver.Chrome("C:\\Users\\ranga\\Downloads\\chromedriver_win32\\chromedriver.exe")
    driver.get(url)
    soup = BeautifulSoup.BeautifulSoup(driver.page_source, 'lxml')
    driver.quit()

    data = soup \
            .findAll('table', {"class": "table l-table table-zebra table-id-mo_sc table-fixed"})[table_bs] \
            .find("tbody", {"cg-busy": "ilc.promise"})
    
    for i in range(len(data.findAll('a'))):
        df.loc[df.shape[0]]=(
            [data.findAll('a')[i].find('span').text, 
         data.findAll('td', {"class": "x-price"})[i].find('span').text, 
         data.findAll('td', {"class": "x-percent"})[i].find('span').text])
    
    return df
	
estimate = 'professional' #   Enter 'consensus' for overall, 
#                     'user_sentiment' for crowd, 
#                     'theoretical' for valuation or 
#                     'professional' for analysts

table_bs = 0 # '0' for buy, '1' for sell
headers = ['Stock', 'Target price', 'Potential']

page_no = 0
df_ = pd.DataFrame(columns=headers)

while(df_.shape[0] % 15 == 0):
    page_no += 1
    url = "https://www.sharewise.com/gb/markets/" + estimate + "/potential_table?page=" + str(page_no) + "&filter%5Bindex%5D=101"
    df_ = get_tab(df_, url)

print(df_)