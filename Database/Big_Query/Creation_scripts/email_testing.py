import smtplib
import datetime 

def time():
	now = datetime.datetime.now()
	date = now.strftime("%Y-%m-%d")
	print("the date is: ",date)
 
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