from google.cloud import bigquery
import datetime as DT
import sys
import smtplib

def analyst_rating():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	subject = ("Analyst rating data collection report for the date range between " + str(week_ago) + "and" + str(today) + ":")
	client = bigquery.Client()
	query_job = client.query("SELECT Constituent, count(*) as number FROM `igenie-project.pecten_dataset_test.analyst_opinions_t` where date BETWEEN TIMESTAMP '{}' and TIMESTAMP'{}' GROUP BY Constituent ORDER BY number".format(week_ago, today))
	
	results = query_job.result()
	body = ""
	#constituent_name = []
	for row in results:
		body = body + row.Constituent +": " + str(row.number) + "\n"
		#s = s+str(row.number)+" news items were inserted for "+row.constituent_name+"\n"
	#message = message + "\n" + s
	message = 'Subject: {}\n\n{}'.format(subject, body)	
	print(message)	
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login(sys.argv[1], sys.argv[2])
	toaddrs = [sys.argv[3],sys.argv[4]]
	server.sendmail(sys.argv[1], toaddrs, message)
	server.quit()
	
if __name__ == '__main__':
	analyst_rating()