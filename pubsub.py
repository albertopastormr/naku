from google.cloud import pubsub_v1
from exception_handler import exception_handler

import numpy as np
from PIL import Image


from io import BytesIO



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
    
    @exception_handler
    def publish_messages(self, project_id, topic_id, ):
        topic_path = self.publisher.topic_path(project_id, topic_id)

        img = Image.open('images/00000001_000.png').resize((256,256), Image.NEAREST)

        rgbimg = Image.new("RGBA", img.size)
        rgbimg.paste(img)

        data = np.asarray(rgbimg)

        print(data)
        print(data.shape)

        np_bytes = BytesIO()
        np.save(np_bytes, data, allow_pickle=True)
        np_bytes = np_bytes.getvalue()


        
        # When you publish a message, the client returns a future.
        future = self.publisher.publish(topic_path, np_bytes)
        print(future.result())

        print(f"Published messages to {topic_path}.")


# docs to consume messages http://www.theappliedarchitect.com/setting-up-gcp-pub-sub-integration-with-python/

if __name__ == "__main__":
    ps = PubSubClient()
    ps.publish_messages("naku-dema", "naku-input-topic")