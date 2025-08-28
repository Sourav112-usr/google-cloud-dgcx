import apache_beam as beam
import csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

HEADERS = [
    "Chest_Pain","Shortness_of_Breath","Fatigue","Palpitations","Dizziness",
    "Swelling_in_Feet_Ankles_Legs","Loss_of_Appetite","Fainting","Weight_Gain",
    "Persistent_Cough","Fast_Heart_Rate","Heart_Murmur","High_Blood_Pressure",
    "Diabetes","Smoking","High_Cholesterol","Family_History_of_Heart_Disease",
    "age","Heart_Risk",
]

def parse_csv(line):
    row = next(csv.reader([line]))
    # pad/truncate to handle ragged lines safely
    if len(row) < len(HEADERS):
        row = row + [""]*(len(HEADERS)-len(row))
    elif len(row) > len(HEADERS):
        row = row[:len(HEADERS)]
    return dict(zip(HEADERS, row))

def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
        runner="DataflowRunner",
        project="chat-bot-dgcx",
        region="asia-south1",
        temp_location="gs://amazon-data-bq/temp/",
        staging_location="gs://amazon-data-bq/staging/",
        # if keeping file loads, ensure this bucket matches the dataset location:
        # experiments=["use_portable_job_submission"],  # optional
    )

    schema = ",".join(f"{h}:STRING" for h in HEADERS)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV" >> beam.io.ReadFromText(
                beam.options.value_provider.RuntimeValueProvider.get_value('input', str)  # if you kept ValueProvider
                if False else  # inline guard if not using it right now
                "gs://amazon-data-bq/raw/heart_disease_risk_dataset_earlymed.csv",
                skip_header_lines=1
            )
            | "Parse" >> beam.Map(parse_csv)
            # QUICK UNBLOCK: STREAMING_INSERTS (swap to FILE_LOADS after fixing temp bucket location)
            | "Write BQ" >> beam.io.WriteToBigQuery(
                table="chat-bot-dgcx:healthcare_heart.heart_attack_factors",
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STREAMING_INSERTS
                # For FILE_LOADS use:
                # method=WriteToBigQuery.Method.FILE_LOADS,
                # custom_gcs_temp_location="gs://bq-tmp-chat-bot-dgcx-asia/tmp",
            )
        )

if __name__ == "__main__":
    run()
