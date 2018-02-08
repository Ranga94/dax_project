from google.cloud import bigquery
import datetime as DT
import sys
import smtplib

####System arguments
##   1 - From Email Id
##   2 - Password of from email id
##   3 - Recipient email id no 1
##   4 - Recipient email id no 2
##   5 - Dataset id
def data_logs(dataset_id):
	today = DT.date.today()
	day_before = today - DT.timedelta(days=1)
	print(day_before)
	subject = ("Data collection report for the date " + str(day_before) + "for" +str(dataset_id) +"is:")
	client = bigquery.Client()
	query_job = client.query("""SELECT Constituent_name, sum(tweets) as tweets, sum(bloomberg) as bloomberg, sum(orbis) as orbis, 
	sum(rss_feeds) as rss_feeds, sum(ticker) as ticker
	FROM `igenie-project.{1}.master_log_table`
	where date = TIMESTAMP("{0}") 
	GROUP By Constituent_name""".format(day_before,dataset_id))
	
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
	data_logs(sys.argv[5])