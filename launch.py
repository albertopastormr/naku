
import numpy as np
from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.vgg16 import preprocess_input
from io import BytesIO
import os
import threading


class LaunchService:

    def __init__(self, broker, project_id, input_topic_id, output_sub_id, directory, run_async):
        self.broker = broker
        self.project_id = project_id
        self.input_topic_id = input_topic_id
        self.output_sub_id = output_sub_id
        self.dir = directory
        self.run_async = run_async

    def __load_img(self, filepath):
        img = image.load_img(filepath, target_size = (224, 224))
        data = image.img_to_array(img)
        data = np.expand_dims(data, axis = 0)
        return preprocess_input(data)

    def __prepare_payload(self, data):
        np_bytes = BytesIO()
        np.save(np_bytes, data, allow_pickle=True)
        np_bytes = np_bytes.getvalue()
        return np_bytes
    
    def __send_images(self):
        for filename in os.listdir(self.dir):
            filepath = f'{self.dir}/{filename}'
            data = self.__load_img(filepath)
            payload = self.__prepare_payload(data)
            print(f'Sending image "{filename}" to topic {self.input_topic_id} in project {self.project_id}')
            self.broker.publish_message(self.project_id, self.input_topic_id, payload, attrs={'filename':filename})
        
    def __retrieve_responses(self):
        self.broker.consume_messages(self.project_id, self.output_sub_id, timeout=None)

    def start(self):
        if self.run_async:
            th_send = threading.Thread(target=self.__send_images)
            #th_retrieve = threading.Thread(target=self.__retrieve_responses)

            th_send.start()
            #th_retrieve.start()
            self.__retrieve_responses()
            th_send.join()
            #th_retrieve.join()
        else:
            self.__send_images()
            self.__retrieve_responses()

