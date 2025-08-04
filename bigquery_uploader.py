from google.cloud import bigquery
from google.oauth2 import service_account
from logger import setup_logger

log = setup_logger()

def upload_dataframe_to_bq(df, project_id, dataset_id, table_id, credentials_path):
    """Upload a pandas DataFrame to BigQuery."""
    try:
        log.info("🔁 Uploading DataFrame to BigQuery...")
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=project_id)

        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()  # Wait for the job to complete

        log.info(f"✅ Upload successful to {table_ref}")
    except Exception as e:
        log.error(f"❌ Failed to upload to BigQuery: {e}")
        raise