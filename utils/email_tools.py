import smtplib
from pymongo import MongoClient
import time

class EmailTools:

    def __init__(self):
        pass

    def send_mail(self, fromaddrs, toaddrs, subject, body, username, password):
        message = 'Subject: {}\n\n{}'.format(subject, body)
        # The actual mail send
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(username, password)
        server.sendmail(fromaddrs, toaddrs, message)
        server.quit()