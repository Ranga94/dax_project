from google.cloud import pubsub
import json
from google.gax.errors import RetryError

class PubsubUtils:
    def __init__(self, google_key_path):
        self.google_key_path = google_key_path
        self.publisher_client = pubsub.PublisherClient()
        self.subscriber_client = pubsub.SubscriberClient()

    def publish(self, project_name, topic_name, data_lines):
        print("Publishing")
        """Publish to the given pubsub topic."""
        pubsub_topic = 'projects/{}/topics/{}'.format(project_name, topic_name)
        messages = []
        for line in data_lines:
            msg_payload = {'data': line}
            messages.append(msg_payload)
        body = {'messages': messages}
        self.publisher_client.publish(pubsub_topic, json.dumps(body).encode('utf-8'))

    def create_subscription(self, project_name, topic_name, sub_name):
        """Creates a new subscription to a given topic."""
        topic_name = 'projects/{}/topics/{}'.format(project_name, topic_name)
        sub_name = 'projects/{}/subscriptions/{}'.format(project_name, sub_name)
        print("using pubsub topic: {}".format(topic_name))
        try:
            subscription = self.subscriber_client.create_subscription(topic_name, sub_name)
            print('Subscription {} was created.'.format(subscription.name))
        except RetryError as e:
            print("Subscription already exists")
            return True
        except Exception as e:
            print(e)
            return None