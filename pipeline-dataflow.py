import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input", type=str, help="GCS input file")
        parser.add_value_provider_argument("--output", type=str, help="BigQuery table")


def transform_data(element):
    # Example: CSV -> Dict
    fields = element.split(",")
    return {
        "id": fields[0],
        "name": fields[1].strip().title(),
        "age": int(fields[2]),
    }


def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
        runner="DataflowRunner",   # For local test, use DirectRunner
        project="chat-bot-dgcx",
        region="us-central1",
        temp_location="gs://banking-data-raw/temp/",
        staging_location="gs://banking-data-raw/staging/",
    )

    options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
            | "Transform" >> beam.Map(transform_data)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=options.output,
                schema="id:STRING, name:STRING, age:INTEGER",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
