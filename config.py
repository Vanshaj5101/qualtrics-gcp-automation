import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
DATA_CENTER = os.getenv("QUALTRICS_DATA_CENTER", "iad1")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")
EXPORT_FORMAT = "csv"
