import smtplib
import datetime as DT

def time():
	#now = datetime.datetime.now()
	#to_date = now.strftime("%Y-%m-%d")
	#week_ago = to_date - now.timedelta(days= 7)
	#from_date = now.strftime("%Y-%m-%d-7")
	today = DT.date.today()
	week_ago = today - DT.timedelta(days=20)
	print("the to_datedate is: ",today)
	print("the from_date is: ",week_ago)
 
def email():
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.starttls()
	server.login("rangavittal2@gmail.com", "rahuldravid")
	toaddrs = ["rangavittalprasad@gmail"]
	msg = "YOUR MESSAGE!"
	server.sendmail("rangavittal2@gmail", toaddrs, msg)
	server.quit()
	
if __name__ == '__main__':
	#email()
	time()