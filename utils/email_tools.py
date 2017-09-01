import smtplib
import project_config
from DB import DB
import time

def send_mail(connection_string, database, toaddrs):
    fromaddr = project_config.mail_username

    database = DB(connection_string, database)
    logging_collection = database.get_collection('tweet_logs')

    result = logging_collection.find_one({'date': time.strftime("%d/%m/%Y")})

    body = str(result)
    subject = "Twitter collection logs: {}".format(time.strftime("%d/%m/%Y"))

    message = 'Subject: {}\n\n{}'.format(subject, body)



    # Credentials (if needed)
    username = project_config.mail_username
    password = project_config.mail_password

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(fromaddr, toaddrs, message)
    server.quit()