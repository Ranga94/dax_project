from google.cloud import bigquery
import datetime as DT
import sys
import smtplib

####System arguments
##   1 - From Email Id
##   2 - Password of from email id
##   3 - Recipient email id no 1
##   4 - Recipient email id no 2

def data_logs():
	#today = DT.date.today()
	today = "2018-02-02"
	#week_ago = today - DT.timedelta(days=7)
	week_ago = "2018-02-02"
	subject = ("Data collection report for the date range between " + str(week_ago) + "and" + str(today) + ":")
	client = bigquery.Client()
	query_job = client.query("""SELECT Constituent_name, sum(tweets) as tweets, sum(bloomberg) as bloomberg, sum(orbis) as orbis, 
	sum(rss_feeds) as rss_feeds, sum(ticker) as ticker
	FROM `igenie-project.pecten_dataset_test.master_log_table`
	where date between TIMESTAMP("{}") and TIMESTAMP("{}")
	GROUP By Constituent_name""".format(week_ago, today))
	
	results = query_job.result()
	body = "Constiuent | Tweets | Bloomberg | Orbis | RSS FEEDS | TICKER" + "\n"
	#constituent_name = []
	for row in results:
		body = body + row.Constituent_name +":tweets: " + str(row.tweets)+ "| bloomberg:" + str(row.bloomberg)+ "| orbis:" + str(row.orbis)+ "| rss:" + str(row.rss_feeds)+"| ticker:" + str(row.ticker) + "\n"
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
	data_logs()