import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:  ",
    "--environment_type=LOOPBACK"
]) # docs: https://beam.apache.org/documentation/runners/spark/

with beam.Pipeline() as pipeline:
    result = (
        pipeline
        | 'Create numbers' >> beam.Create([1, 2, 3, 4])
        | 'Multiply by two' >> beam.Map(lambda x: x * 2)
        | 'Sum everything' >> beam.CombineGlobally(sum)
        | 'Print results' >> beam.Map(print)
    )
