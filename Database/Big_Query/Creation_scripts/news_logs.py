from google.cloud import bigquery
import datetime as DT
import sys
import smtplib


def news_log_read():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	subject = ("News collection report for the date range between " + str(week_ago) + " and " +str(today)+":")
	client = bigquery.Client()
	query_job = client.query("SELECT constituent_name, sum(downloaded_news) as number FROM `igenie-project.pecten_dataset_new.news_logs` where date BETWEEN TIMESTAMP '{}' and TIMESTAMP'{}' GROUP BY constituent_name ORDER BY number".format(week_ago, today))
		
	results = query_job.result()
	body = ""
	#constituent_name = []
	for row in results:
		body = body + row.constituent_name +": " + str(row.number) + "\n"
		#s = s+str(row.number)+" news items were inserted for "+row.constituent_name+"\n"
	#message = message + "\n" + s
	message = 'Subject: {}\n\n{}'.format(subject, body)	
	print(message)
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login(sys.argv[1], sys.argv[2])
	toaddrs = [sys.argv[3]]
	server.sendmail(sys.argv[1], toaddrs, message)
	server.quit()
		
if __name__ == '__main__':
	news_log_read()
