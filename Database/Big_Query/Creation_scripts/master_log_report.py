from google.cloud import bigquery
import datetime as DT
import sys
import smtplib

def twitter_logs():
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=7)
	subject = ("Tweet collection report for the date range between " + str(week_ago) + "and" + str(today) + ":")
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
		body = body + row.Constituent_name +":| " + str(row.tweets)+ "|" + str(row.bloomberg)+ "|" + str(row.orbis)+ "|" + str(row.rss_feeds)+"|" + str(row.ticker) + "\n"
		#s = s+str(row.number)+" news items were inserted for "+row.constituent_name+"\n"
	#message = message + "\n" + s
	message = 'Subject: {}\n\n{}'.format(subject, body)	
	print(message)	
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login("rangavittal2@gmail.com", "rahuldravid")
	toaddrs = ["ranga@igenieconsulting.com","rangavittalprasad@gmail.com"]
	server.sendmail("rangavittal2@gmail.com", toaddrs, message)
	server.quit()
	
if __name__ == '__main__':
	twitter_logs()