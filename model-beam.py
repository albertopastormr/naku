import logging
import argparse

import apache_beam as beam


from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupWindowsIntoBatches(beam.PTransform):
    """
    A composite transform that groups Pub/Sub messages
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals"
            >> beam.WindowInto(FixedWindows(self.window_size))
        )

class PredictLabel(beam.DoFn):

    def process(self, element):
        """pubsub input is a byte string"""
        data = element.decode('utf-8')
        """do some custom transform here"""
        data = data.encode('utf-8')
        yield data


def run(project_id, input_sub, output_topic, window_size=1.0, num_shards=5, pipeline_args=None):
    options = PipelineOptions([
        "--runner=PortableRunner",
        "--job_endpoint=localhost:7077",
        "--environment_type=LOOPBACK"
    ]) # docs: https://beam.apache.org/documentation/runners/spark/
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from input topic (PubSub)' >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{input_sub}')
            | "Window into" >> GroupWindowsIntoBatches(window_size)
            | 'Predict the label of each image' >> beam.ParDo(PredictLabel())
            #| 'Print results' >> beam.Map(print)
            | 'Write to output topic' >>  beam.io.WriteToPubSub(topic=f'projects/{project_id}/topics/{output_topic}')
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
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )

    known_args, pipeline_args = parser.parse_known_args()

    print(known_args)
    print(pipeline_args)
    run(project_id=known_args.project_id, input_sub=known_args.input_sub, output_topic=known_args.output_topic,
        window_size=known_args.window_size, pipeline_args="--runner DirectRunner")