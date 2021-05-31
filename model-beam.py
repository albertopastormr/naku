import logging
import argparse

import numpy as np
from io import BytesIO

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import shared

from tensorflow.keras.applications.vgg16 import VGG16, decode_predictions

class WeakRefModel():
   def __init__(self, model):
       self.model=model

class DoManualInference(beam.DoFn):
   def __init__(self, shared_handle, saved_model_path=None):
       self._shared_handle = shared_handle
       self._saved_model_path = saved_model_path # TODO: check this to use pretrained models
       # docs: https://cloud.google.com/blog/products/data-analytics/ml-inference-in-dataflow-pipelines
       # docs: https://beam.apache.org/releases/pydoc/2.24.0/apache_beam.utils.shared.html
       # docs: https://keras.io/api/applications/vgg/
  
   def setup(self):
       def initialize_model():
            # Load a potentially large model in memory. Executed once per process, it can be shared across workers
            #return WeakRefModel(tf.saved_model.load(self._saved_model_path, ["serve"])) TODO: check this to use pretrained models
            return VGG16()
      
       self._weakRefModel = self._shared_handle.acquire(initialize_model)
  
   def process(self, input_batch):
        for idx, x in enumerate(input_batch):
            data = x.data
            filename = x.attributes['filename']
            
            load_bytes = BytesIO(data)
            img = np.load(load_bytes, allow_pickle=True)

            prediction = self._weakRefModel.predict(img)
            prediction_label = decode_predictions(prediction, top = 5)
            label = '%s (%.2f%%)' % (prediction_label[0][0][1], prediction_label[0][0][2]*100 )
            print(idx, label) # used within gcp console for debugging
            
            record = beam.io.PubsubMessage(label.encode("utf-8"), {'filename':filename})
            yield record


def run(project_id, input_sub, output_topic, pipeline_args=None):
    options = PipelineOptions([
        "--runner=PortableRunner",
        "--job_endpoint=localhost:7077",
        "--environment_type=LOOPBACK"
    ]) # docs: https://beam.apache.org/documentation/runners/spark/ # TODO: check this to try Spark runner (portable runner)
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        shared_handle = shared.Shared()
        (
            pipeline
            | 'Read from input topic (PubSub)' >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{input_sub}', with_attributes=True)
            | 'Batch elements' >> beam.BatchElements(min_batch_size=1, max_batch_size=10)
            | 'Predict the label of each image' >> beam.ParDo(DoManualInference(shared_handle=shared_handle))
            | 'Write to output topic' >>  beam.io.WriteToPubSub(topic=f'projects/{project_id}/topics/{output_topic}', with_attributes=True)
        )

if __name__ =="__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_sub",
        help="The Cloud Pub/Sub topic to read from.",
    )
    parser.add_argument(
        '--project_id',
        help="your Google Cloud project ID")
    parser.add_argument(
        "--output_topic",
        help="The Cloud Pub/Sub topic to write to.",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(project_id=known_args.project_id, input_sub=known_args.input_sub, output_topic=known_args.output_topic, pipeline_args="--runner DirectRunner")