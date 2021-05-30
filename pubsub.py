from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
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
    
    @exception_handler
    def publish_message(self, project_id, topic_id, payload, attrs):
        topic_path = self.publisher.topic_path(project_id, topic_id)

        future = self.publisher.publish(topic_path, payload, **attrs)
        future.result()

        print(f"Published message to {topic_path}.")

    @exception_handler
    def consume_messages(self, project_id, sub_id, timeout):
        subscription_path = self.subscriber.subscription_path(project_id, sub_id)
        
        def callback(message):
            print(f"Received message {message.data.decode('utf-8')} [{message.attributes}]")
            message.ack()

        streaming_pull_future = self.subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")
        try:
                streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
                streaming_pull_future.cancel()
                print(f'Timeout on {subscription_path} ({timeout} seconds)')

# docs to consume messages http://www.theappliedarchitect.com/setting-up-gcp-pub-sub-integration-with-python/

if __name__ == "__main__":
    ps = PubSubClient()
    ps.publish_messages("naku-dema", "naku-input-topic")