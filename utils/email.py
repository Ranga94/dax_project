import smtplib
import project_config
from DB import DB

def send_mail(connection_string, database):
    fromaddr = project_config.mail_username
    toaddrs = ["ulysses@igenieconsulting.com"]

    database = DB(connection_string, database)
    collec

    msg = 'There was a terrible error that occured and I wanted you to know!'

    # Credentials (if needed)
    username = 'username'
    password = 'password'

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(fromaddr, toaddrs, msg)
    server.quit()