from google.cloud import bigquery
import datetime as DT
import sys
import smtplib
import pandas as pd



def news_log_read():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	print("News collection report for the date range between {} and {}:".format(week_ago, today))
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, count(*) as number FROM pecten_dataset_test.news_logs where date between TIMESTAMP'{}' and TIMESTAMP '{}' GROUP BY constituent_name ORDER BY number".format(week_ago, today))
		
	results = query_job.result()
	s = ""
	#constituent_name = []
	for row in results:
		s += ("'{}' news items were inserted for '{}'"+"\n".format(row.number,row.constituent_name))
	#	constituent_name.append(row.constituent_name)
	#for i, j in zip(number, constituent_name):
	#	msg = "{} news items were inserted for {} \n".format(i,j)
	print(s)
	"""server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login("rangavittal2@gmail.com", sys.argv[1])
	toaddrs = [sys.argv[2]]
	server.sendmail("rangavittal2@gmail", toaddrs, msg)
	server.quit()"""
		
if __name__ == '__main__':
	news_log_read()
