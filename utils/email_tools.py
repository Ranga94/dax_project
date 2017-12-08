from . import Storage
import smtplib
from .twitter_analytics_helpers import *
from pprint import pformat

def send_mail(param_connection_string, google_key_path, logging_process,
              param_table, param_where, query1, query2, param_columns=["EMAIL_USERNAME", "EMAIL_PASSWORD"]):
    storage = Storage.Storage(google_key_path=google_key_path)

    parameters = get_parameters(param_connection_string, param_table,param_columns, param_where)

    latest_logs = storage.get_bigquery_data(query=query1, iterator_flag=False)
    latest_logs_list = [l.values() for l in latest_logs]

    total_items = storage.get_bigquery_data(query=query2, iterator_flag=False)
    total_items_list = [l.values() for l in total_items]

    body = "Latest {} collected\n".format(logging_process) + pformat(latest_logs_list) + "\n\n\n" + \
            "Total {}\n".format(logging_process) + pformat(total_items_list)
    subject = "{} collection logs: {}".format(logging_process,time.strftime("%d/%m/%Y"))

    message = 'Subject: {}\n\n{}'.format(subject, body)

    # Credentials (if needed)
    username = parameters["EMAIL_USERNAME"]
    password = parameters["EMAIL_PASSWORD"]

    #toaddrs = ["ulysses@igenieconsulting.com", "twitter@igenieconsulting.com"]
    toaddrs = ["ulysses@igenieconsulting.com"]

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(username, toaddrs, message)
    server.quit()