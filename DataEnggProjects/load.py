from google.cloud import bigquery
import os
import glob

# Set the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/arvindranganathraghuraman/Desktop/DataEnggProjects/enduring-rush-447904-v2-e5aad08ac6ca.json"

def upload_csv_to_bigquery(csv_file_path, project_id, dataset_id, table_id):
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        print(f"Initialized BigQuery client for project: {project_id}")

        # Define the table reference
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )

        # Upload the CSV file
        with open(csv_file_path, "rb") as source_file:
            load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            load_job.result()  # Wait for the load job to complete

        print(f"CSV file successfully uploaded to BigQuery table {table_ref}.")
    except Exception as e:
        print(f"An error occurred while uploading the CSV: {e}")
        print(f"Error details: {str(e)}")

if __name__ == "__main__":
    # List all CSV files in the directory
    csv_files = ['datetime_dim.csv','dropoff_location_dim.csv','fact_table.csv','passenger_count_dim.csv','payment_type_dim.csv','pickup_location_dim.csv','rate_code_dim.csv','trip_distance_dim.csv']
    print("CSV files found:", csv_files)

    # Replace with your GCP project and dataset details
    project_id = "enduring-rush-447904-v2"
    dataset_id = "UberDatasetarv"

    if not csv_files:
        print("No CSV files found. Check the directory path.")
    else:
        for csv_file_path in csv_files:
            table_id = os.path.splitext(os.path.basename(csv_file_path))[0]
            print(f"Uploading {csv_file_path} to table {table_id}...")
            upload_csv_to_bigquery(csv_file_path, project_id, dataset_id, table_id)
