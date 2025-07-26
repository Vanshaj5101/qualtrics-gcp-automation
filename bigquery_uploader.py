from google.cloud import bigquery

def upload_to_bigquery(df, project_id, dataset_id, table_id, credentials_path=None):
    if credentials_path:
        client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
    else:
        client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for completion
    print(f"✅ Uploaded {len(df)} rows to {table_ref}")
