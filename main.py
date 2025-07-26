from config import SURVEY_ID
from qualtrics_api import verify_authentication, initiate_export, wait_for_export, download_responses
from transformer import clean_dataframe
from column_mapping import COLUMN_MAPPING
from logger import setup_logger
from bigquery_uploader import upload_to_bigquery
from datetime import datetime
import os

log = setup_logger()

def run_pipeline():
    try:
        verify_authentication()
        progress_id = initiate_export(SURVEY_ID)
        file_id = wait_for_export(SURVEY_ID, progress_id)
        raw_df = download_responses(SURVEY_ID, file_id)
        final_df = clean_dataframe(raw_df, COLUMN_MAPPING)
        log.info("✅ DataFrame ready.")
        log.info(final_df.head())
        log.info(f"Shape: {final_df.shape}")
        return final_df
    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    df = run_pipeline()

    # # Optional: export as CSV
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # output_dir = "exports"
    # os.makedirs(output_dir, exist_ok=True)
    # csv_path = os.path.join(output_dir, f"qualtrics_data_{timestamp}.csv")
    # df.to_csv(csv_path, index=False)
    # log.info(f"📁 Data exported to CSV: {csv_path}")

    # df = df.astype(str)

    # for col in df.columns:
    #     if df[col].dtype == "object":
    #         types = df[col].map(type).unique()
    #         if len(types) > 1:
    #             print(f"⚠️ Mixed types in column '{col}': {types}")


    # upload_to_bigquery(
    #     df,
    #     project_id="qualtrics-data-pipeline",
    #     dataset_id="qualtrics_data",
    #     table_id="end_of_course_survey_responses",
    #     credentials_path="qualtrics-data-uploader-key.json"  # You’ll generate this in GCP setup
    # )
