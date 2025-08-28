import apache_beam as beam
import csv
from apache_beam.options.pipeline_options import PipelineOptions


class DataflowOptions(PipelineOptions):
@classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--input", type=str, help="GCS input file")
        parser.add_value_provider_argument("--output", type=str, help="BigQuery table")
    
    
    def parse_csv(line, headers):
        row = next(csv.reader([line]))
        return dict(zip(headers, row))
    
    
    def transform_data(element):
    # Convert numeric fields properly
        element["age"] = int(element["age"]) if element["age"].isdigit() else None
        return element
    
    
    def run():
        pipeline_options = PipelineOptions(
            save_main_session=True,
            runner="DataflowRunner",   # change to DirectRunner for local test
            project="chat-bot-dgcx",
            region="asia-south1",
            temp_location="gs://amazon-data-bq/temp/",
            staging_location="gs://amazon-data-bq/staging/",
        )
        
        options = pipeline_options.view_as(DataflowOptions)
        
        headers = [
            "Chest_Pain",
            "Shortness_of_Breath",
            "Fatigue",
            "Palpitations",
            "Dizziness",
            "Swelling_in_Feet_Ankles_Legs",
            "Loss_of_Appetite",
            "Fainting",
            "Weight_Gain",
            "Persistent_Cough",
            "Fast_Heart_Rate",
            "Heart_Murmur",
            "High_Blood_Pressure",
            "Diabetes",
            "Smoking",
            "High_Cholesterol",
            "Family_History_of_Heart_Disease",
            "age",
            "Heart_Risk",
        ]
    
        with beam.Pipeline(options=pipeline_options) as p:
            (
                p
                | "Read data" >> beam.io.ReadFromText(options.input, skip_header_lines=1)
                | "Parse CSV" >> beam.Map(lambda line: parse_csv(line, headers))
                | "Transform" >> beam.Map(transform_data)
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table=options.output,
                    schema=(
                        "Chest_Pain:STRING,"
                        "Shortness_of_Breath:STRING,"
                        "Fatigue:STRING,"
                        "Palpitations:STRING,"
                        "Dizziness:STRING,"
                        "Swelling_in_Feet_Ankles_Legs:STRING,"
                        "Loss_of_Appetite:STRING,"
                        "Fainting:STRING,"
                        "Weight_Gain:STRING,"
                        "Persistent_Cough:STRING,"
                        "Fast_Heart_Rate:STRING,"
                        "Heart_Murmur:STRING,"
                        "High_Blood_Pressure:STRING,"
                        "Diabetes:STRING,"
                        "Smoking:STRING,"
                        "High_Cholesterol:STRING,"
                        "Family_History_of_Heart_Disease:STRING,"
                        "age:INTEGER,"
                        "Heart_Risk:STRING"
                    ),
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


if __name__ == "__main__":
run()
