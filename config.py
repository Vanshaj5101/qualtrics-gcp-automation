import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
DATA_CENTER = os.getenv("QUALTRICS_DATA_CENTER", "iad1")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")
EXPORT_FORMAT = "csv"


# BigQuery configuration
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS_PATH")

# VG BigQuery configuration
VG_BQ_PROJECT_ID = os.getenv("VG_BQ_PROJECT_ID")
VG_BQ_DATASET_ID = os.getenv("VG_BQ_DATASET_ID")
VG_BQ_TABLE_ID = os.getenv("VG_BQ_TABLE_ID")
VG_BQ_CREDENTIALS_PATH = os.getenv("VG_BQ_CREDENTIALS_PATH")