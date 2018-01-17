from google.cloud import bigquery
import datetime as DT
import sys
import smtplib
import pandas as pd



def news_log_read():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	message = ("News collection report for the date range between" + str(week_ago) + "and" +str(today)+":")
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, count(*) as number FROM pecten_dataset_test.news_logs where date between TIMESTAMP'{}' and TIMESTAMP '{}' GROUP BY constituent_name ORDER BY number".format(week_ago, today))
		
	results = query_job.result()
	s = ""
	#constituent_name = []
	for row in results:
		s = s + row.constituent_name +": " + row.number
		#s = s+str(row.number)+" news items were inserted for "+row.constituent_name+"\n"
	message = message + "\n" + s	
	
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login("rangavittal2@gmail.com", sys.argv[1])
	toaddrs = [sys.argv[2]]
	server.sendmail("rangavittal2@gmail", toaddrs, message)
	server.quit()
		
if __name__ == '__main__':
	news_log_read()
