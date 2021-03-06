
import numpy as np
import os
import threading


class LaunchService:

    def __init__(self, broker, project_id, input_topic_id, output_sub_id, directory, run_async, image_size, timeout=None):
        self.broker = broker
        self.project_id = project_id
        self.input_topic_id = input_topic_id
        self.output_sub_id = output_sub_id
        self.dir = directory
        self.run_async = run_async
        self.timeout = timeout
        self.image_size = image_size

    def __load_img(self, filepath, image_size):
        from tensorflow.keras.preprocessing import image
        from tensorflow.keras.applications.vgg16 import preprocess_input

        img = image.load_img(filepath, target_size = image_size)
        data = image.img_to_array(img)
        data = np.expand_dims(data, axis = 0)
        return preprocess_input(data)

    def __prepare_payload(self, data):
        from io import BytesIO

        np_bytes = BytesIO()
        np.save(np_bytes, data, allow_pickle=True)
        np_bytes = np_bytes.getvalue()
        return np_bytes
    
    def __send_images(self):
        for filename in os.listdir(self.dir):
            filepath = f'{self.dir}/{filename}'
            data = self.__load_img(filepath, self.image_size)
            payload = self.__prepare_payload(data)
            print(f'Sending image "{filename}" to topic {self.input_topic_id} in project {self.project_id}')
            self.broker.publish_message(self.project_id, self.input_topic_id, payload, attrs={'filename':filename})
        
    def __retrieve_responses(self):
        self.broker.consume_messages(self.project_id, self.output_sub_id, timeout=self.timeout)

    def start(self):
        if self.run_async:
            th_send = threading.Thread(target=self.__send_images)
            th_send.start()
            self.__retrieve_responses()
            th_send.join()
        else:
            self.__send_images()
            self.__retrieve_responses()

