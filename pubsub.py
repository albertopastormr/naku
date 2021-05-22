from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import AlreadyExists, PermissionDenied
from google.cloud import pubsub_v1

import sys

def exception_handler(func):

    def _handle_errors(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AlreadyExists:
            print(f"WARNING: Resource '{args[2]}' already exists")
        except DefaultCredentialsError:
            print("ERROR: Could not automatically determine credentials, launch auth option first")
            sys.exit(1)
        except PermissionDenied:
            print('ERROR: You have to enable the PubSub API: https://console.developers.google.com/apis/api/pubsub.googleapis.com/'
            ' and the Dataproc API: https://console.cloud.google.com/apis/library/dataproc.googleapis.com'
            f' for the project {args[1]}')
            sys.exit(1)
            
    return _handle_errors


class PubSubClient:
    
    @exception_handler
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
    
    @exception_handler
    def create_topic(self, project_id, topic_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)

       
        topic = self.publisher.create_topic(request={"name": topic_path})
        print("Created topic: {}".format(topic.name))

    def delete_topic(self, project_id, topic_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)

        
        self.publisher.delete_topic(request={"topic": topic_path})
        print("Topic deleted: {}".format(topic_path))
        
    @exception_handler
    def list_topics(self, project_id):
        project_path = f"projects/{project_id}"

        print(f"Listing existing topics in project {project_id}")
        for topic in self.publisher.list_topics(request={"project": project_path}):
            print("--> " + topic.name)

    def publish_messages(self, project_id, topic_id):
        pass

    def create_subscription(self, project_id, topic_id, subscription_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

        with self.subscriber:
            subscription = self.subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )

        print(f"Subscription created: {subscription}")

    def delete_subscription(self, project_id, subscription_id):
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

        with self.subscriber:
            self.subscriber.delete_subscription(request={"subscription": subscription_path})
        
        print(f"Subscription deleted: {subscription_path}.")

    def list_subscriptions(self, project_id):
        project_path = f"projects/{project_id}"

        with self.subscriber:
            print(f"Listing existing subscriptions in project {project_id}")
            for subscription in self.subscriber.list_subscriptions(
                request={"project": project_path}
            ):
                print("--> " + subscription.name)