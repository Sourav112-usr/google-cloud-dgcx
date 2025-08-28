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
        "Chest_Pain": fields[0],
        "Shortness_of_Breath": fields[1],
        "Fatigue": fields[2],
        "Palpitations": fields[3],
        "Dizziness": fields[4],
        "age": fields[17],
        "Heart_Risk": fields[18]
    }


def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
        runner="DataflowRunner",   # For local test, use DirectRunner
        project="chat-bot-dgcx",
        region="asia-south1",
        temp_location="gs://amazon-data-bq/temp/",
        staging_location="gs://amazon-data-bq/staging/",
    )

    options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from GCS" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
            | "Transform" >> beam.Map(transform_data)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=options.output,
                schema="Chest_Pain:BINARY,	Shortness_of_Breath:BINARY,	Fatigue:BINARY,	Palpitations:BINARY, Dizziness:BINARY, age:INTEGER, Heart_Risk:BINARY",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
