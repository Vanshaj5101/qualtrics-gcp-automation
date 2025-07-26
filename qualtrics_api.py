import time
import zipfile
import io
import pandas as pd
import requests
from config import API_TOKEN, DATA_CENTER, EXPORT_FORMAT
from logger import setup_logger

log = setup_logger()

def verify_authentication() -> None:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/whoami"
    resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
    if resp.ok:
        user = resp.json()["result"]
        log.info(f"Authenticated as {user['userId']} (Brand: {user['brandId']})")
    else:
        log.error("Authentication failed.")
        resp.raise_for_status()

def initiate_export(survey_id: str) -> str:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses"
    resp = requests.post(
        url,
        headers={"X-API-TOKEN": API_TOKEN, "Content-Type": "application/json"},
        json={"format": EXPORT_FORMAT}
    )
    resp.raise_for_status()
    progress_id = resp.json()["result"]["progressId"]
    log.info(f"Export initiated (Progress ID: {progress_id})")
    return progress_id

def wait_for_export(survey_id: str, progress_id: str) -> str:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{progress_id}"
    while True:
        time.sleep(2)
        resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
        resp.raise_for_status()
        status = resp.json()["result"]["status"]
        log.info(f"Export Status: {status}")
        if status == "complete":
            return resp.json()["result"]["fileId"]
        elif status == "failed":
            raise RuntimeError("Export failed.")

def download_responses(survey_id: str, file_id: str) -> pd.DataFrame:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{file_id}/file"
    resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f)
    return df
