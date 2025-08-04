import config
from qualtrics_api import verify_authentication, initiate_export, wait_for_export, download_responses
from transformer import clean_dataframe
from column_mapping import COLUMN_MAPPING
from logger import setup_logger
from bigquery_uploader import upload_dataframe_to_bq
from datetime import datetime

log = setup_logger()

def run_pipeline():
    try:
        verify_authentication()
        progress_id = initiate_export(config.SURVEY_ID)
        file_id = wait_for_export(config.SURVEY_ID, progress_id)
        raw_df = download_responses(config.SURVEY_ID, file_id)
        final_df = clean_dataframe(raw_df, COLUMN_MAPPING)
        log.info("✅ DataFrame ready.")
        log.info(final_df.head())
        log.info(f"Shape: {final_df.shape}")
        # uploading to LESF BigQuery
    #     upload_dataframe_to_bq(
    #     final_df,
    #     project_id=config.BQ_PROJECT_ID,
    #     dataset_id=config.BQ_DATASET_ID,
    #     table_id=config.BQ_TABLE_ID,
    #     credentials_path=config.BQ_CREDENTIALS_PATH  # You’ll generate this in GCP setup
    # )
        # upload to VG BigQuery
        upload_dataframe_to_bq(
            final_df,
            project_id=config.VG_BQ_PROJECT_ID,
            dataset_id=config.VG_BQ_DATASET_ID,
            table_id=config.VG_BQ_TABLE_ID,
            credentials_path=config.VG_BQ_CREDENTIALS_PATH
        )
        # return final_df
    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()

    # # Optional: export as CSV
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # output_dir = "exports"
    # os.makedirs(output_dir, exist_ok=True)
    # csv_path = os.path.join(output_dir, f"qualtrics_data_{timestamp}.csv")
    # df.to_csv(csv_path, index=False)
    # log.info(f"📁 Data exported to CSV: {csv_path}")


    
