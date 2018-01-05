from selenium import webdriver
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient, errors
from datetime import datetime, timedelta
from requests.compat import urljoin
import time
import sys

driver = webdriver.PhantomJS()
url = "https://sparklingocean.shinyapps.io/Dashboard_test/"
driver.get(url)
time.sleep(20)
driver.find_element_by_xpath("/html/body/div[1]/aside/section/ul/div/div/div/div[1]").click()
time.sleep(10)
print(driver.page_source)


'''
if "Twitter Sentiment Count" in driver.page_source:
    print("Ok")

soup = BeautifulSoup(driver.page_source, 'lxml')
print(soup.contents)

'''