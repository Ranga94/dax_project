from google.cloud import bigquery
import datetime as DT
import sys
import smtplib

def twitter_logs():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	subject = ("Tweet collection report for the date range between " + str(week_ago) + "and" + str(today) + ":")
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, sum(downloaded_tweets) as number FROM `igenie-project.pecten_dataset_new.twee_logs` where date BETWEEN TIMESTAMP '{}' and TIMESTAMP'{}' GROUP BY constituent_name ORDER BY number".format(week_ago, today))
	
	results = query_job.result()
	body = ""
	#constituent_name = []
	for row in results:
		body = body + row.constituent_name +": " + str(row.number) + "\n"
		#s = s+str(row.number)+" news items were inserted for "+row.constituent_name+"\n"
	#message = message + "\n" + s
	message = 'Subject: {}\n\n{}'.format(subject, body)	
	print(message)	
	