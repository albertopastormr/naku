from google.cloud import pubsub_v1
from exception_handler import exception_handler

class PubSubClient:
    
    @exception_handler
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
    
    @exception_handler
    def create_topic(self, project_id, topic_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)

       
        topic = self.publisher.create_topic(request={"name": topic_path})
        print(f"Topic created: {topic.name}")
    
    @exception_handler
    def delete_topic(self, project_id, topic_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)

        
        self.publisher.delete_topic(request={"topic": topic_path})
        print(f"Topic deleted: {topic_path}")
        
    @exception_handler
    def list_topics(self, project_id):
        project_path = f"projects/{project_id}"

        print(f"Listing existing topics in project {project_id}")
        for topic in self.publisher.list_topics(request={"project": project_path}):
            print("  --> " + topic.name)

    def publish_messages(self, project_id, topic_id):
        pass
    
    @exception_handler
    def create_subscription(self, project_id, topic_id, subscription_id):
        topic_path = self.publisher.topic_path(project_id, topic_id)
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

        subscription = self.subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )

        print(f"Subscription created: {subscription.name}")

    @exception_handler
    def delete_subscription(self, project_id, subscription_id):
        subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

        self.subscriber.delete_subscription(request={"subscription": subscription_path})
        
        print(f"Subscription deleted: {subscription_path}")

    @exception_handler
    def list_subscriptions(self, project_id):
        project_path = f"projects/{project_id}"

        print(f"Listing existing subscriptions in project {project_id}")
        for subscription in self.subscriber.list_subscriptions(
            request={"project": project_path}
        ):
            print("  --> " + subscription.name)