import os
import time
import requests
import zipfile
import io
import pandas as pd
import logging
from typing import Dict
from dotenv import load_dotenv

# ────────────────────────
# Load Configs from .env
# ────────────────────────
load_dotenv()
API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
DATA_CENTER = os.getenv("QUALTRICS_DATA_CENTER", "iad1")
SURVEY_ID = os.getenv("QUALTRICS_SURVEY_ID")
EXPORT_FORMAT = "csv"

# ────────────────────────
# Setup Logging
# ────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ────────────────────────
# Column Mapping Dictionary
# ────────────────────────
COLUMN_MAPPING: Dict[str, str] = {
    "StartDate": "start_date",
    "EndDate": "end_date",
    "Status": "response_status",
    "IPAddress": "ip_address",
    "Progress": "progress_percent",
    "Duration (in seconds)": "duration_seconds",
    "Finished": "is_finished",
    "RecordedDate": "recorded_date",
    "ResponseId": "response_id",
    "RecipientLastName": "recipient_last_name",
    "RecipientFirstName": "recipient_first_name",
    "RecipientEmail": "recipient_email",
    "ExternalReference": "external_reference",
    "LocationLatitude": "location_latitude",
    "LocationLongitude": "location_longitude",
    "DistributionChannel": "distribution_channel",
    "UserLanguage": "user_language",
    "Q1_1": "reason_for_course_selection_college_credits",
    "Q1_2": "reason_for_course_selection_specific_credits",
    "Q1_4": "reason_for_course_selection_career_prospects",
    "Q1_5": "reason_for_course_selection_pathway",
    "Q1_3": "reason_for_course_selection_interest",
    "Q1_6": "reason_for_course_selection_other",
    "Q1_6_TEXT": "reason_for_course_selection_other_text",
    "Q2.1_1": "satisfaction_course_rating",
    "Q2_1": "recommendation_rating",
    "Q2.2": "satisfaction_course_rating_text",
    "Q3_1": "challenge_academic_support",
    "Q3_2": "challenge_understanding_content",
    "Q3_3": "challenge_learning_tools",
    "Q3_4": "challenge_understanding_grades",
    "Q3_5": "challenge_navigating_canvas",
    "Q3_6": "challenge_getting_feedback",
    "Q3_8": "challenge_technical_requirements_proctoring_exams",
    "Q3_9": "challenge_other",
    "Q3_9_TEXT": "challenge_other_text",
    "Q4_1": "agree_content_useful_for_education",
    "Q4_2": "agree_content_relevant_to_career",
    "Q4_3": "agree_workload_reasonable",
    "Q4_4": "agree_deadlines_reasonable",
    "Q4_5": "agree_content_relevant_to_personal_experience",
    "Q4_6": "agree_assessments_alignment_with_course",
    "Q5_1": "useful_video_lectures",
    "Q5_2": "useful_reading_materials",
    "Q5_3": "useful_discussion_boards",
    "Q5_4": "useful_interactive_tools",
    "Q5_5": "useful_projects",
    "Q5_6": "useful_reflection_journaling",
    "Q5_7": "useful_engagement",
    "Q6": "course_feedback_liked_best",
    "Q7": "course_feedback_improvement_suggestions_text",
    "Q8": "willing_followup_call",
    "Q8.1": "is_18_or_older",
    "Q8.2": "followup_email",
    "uid": "user_id",
    "course_id": "course_id",
    "outcomes_id": "outcomes_id"
}

# ────────────────────────
# Qualtrics API Functions
# ────────────────────────

def verify_authentication() -> None:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/whoami"
    resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
    if resp.ok:
        user = resp.json()["result"]
        logging.info(f"Authenticated as {user['userId']} (Brand: {user['brandId']})")
    else:
        logging.error("Authentication failed. Check API token or data center.")
        resp.raise_for_status()


def initiate_export(survey_id: str) -> str:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses"
    resp = requests.post(url, headers={"X-API-TOKEN": API_TOKEN, "Content-Type": "application/json"}, json={"format": EXPORT_FORMAT})
    resp.raise_for_status()
    progress_id = resp.json()["result"]["progressId"]
    logging.info(f"Export initiated (Progress ID: {progress_id})")
    return progress_id


def wait_for_export(survey_id: str, progress_id: str) -> str:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{progress_id}"
    while True:
        time.sleep(2)
        resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
        resp.raise_for_status()
        result = resp.json()["result"]
        logging.info(f"Export Status: {result['status']}")
        if result["status"] == "complete":
            return result["fileId"]
        elif result["status"] == "failed":
            raise RuntimeError("Export failed.")


def download_responses(survey_id: str, file_id: str) -> pd.DataFrame:
    url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{file_id}/file"
    resp = requests.get(url, headers={"X-API-TOKEN": API_TOKEN})
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            full_df = pd.read_csv(f)
    return full_df


# ────────────────────────
# Data Processing
# ────────────────────────

def clean_dataframe(df: pd.DataFrame, col_mapping: Dict[str, str]) -> pd.DataFrame:
    # Step 1: Rename columns using the raw Qualtrics names
    df_renamed = df.rename(columns=col_mapping)

    # Step 2: Remove the first 2 rows (question text and metadata)
    df_clean = df_renamed.iloc[2:].reset_index(drop=True)

    # Step 3: Map satisfaction rating to human-readable labels
    satisfaction_map = {
        '1': 'Highly Dissatisfied',
        '2': 'Dissatisfied',
        '3': 'Neither Satisfied nor Dissatisfied',
        '4': 'Satisfied',
        '5': 'Highly Satisfied'
    }

    # Handle nulls and map values
    df_clean["satisfaction_course_rating_label"] = (
            df_clean["satisfaction_course_rating"]
            .apply(lambda x: satisfaction_map.get(str(int(x))) if pd.notnull(x) else "Null")
    )

    # Step 4: Map recommendation rating to human-readable labels
    recommendation_map = {
        '1': 'Highly Unlikely',
        '2': 'Unlikely',
        '3': 'Neither Likely nor Unlikely',
        '4': 'Likely',
        '5': 'Highly Likely'
    }

    df_clean["recommendation_rating_label"] = (
        df_clean["recommendation_rating"]
        .apply(lambda x: recommendation_map.get(str(int(x))) if pd.notnull(x) else "Null")
    )

    # Step 5: Map usefulness of video lectures to human-readable labels
    usefulness_map = {
        '1': 'Not at all Useful',
        '2': 'Not very Useful',
        '3': 'Neutral',
        '4': 'Moderately Useful',
        '5': 'Extremely Useful'
    }

    usefulness_columns = {
        "useful_video_lectures": "useful_video_lectures_label",
        "useful_reading_materials": "useful_reading_materials_label",
        "useful_discussion_boards": "useful_discussion_boards_label",
        "useful_interactive_tools": "useful_interactive_tools_label",
        "useful_projects": "useful_projects_label",
        "useful_reflection_journaling": "useful_reflection_journaling_label",
        "useful_engagement": "useful_engagement_label"
    }


    for source_col, target_col in usefulness_columns.items():
        df_clean[target_col] = (
            df_clean[source_col]
            .apply(lambda x: usefulness_map.get(str(int(x))) if pd.notnull(x) else "Null")
    )


    # Step 6: Map agreement ratings to human-readable labels
    agree_standard_map = {
        '1': 'Strongly Disagree',
        '2': 'Disagree',
        '3': 'Neither Agree nor Disagree',
        '4': 'Agree',
        '5': 'Strongly Agree'
    }

    agree_reversed_map = {
        '1': 'Strongly Agree',
        '2': 'Agree',
        '3': 'Neither Agree nor Disagree',
        '4': 'Disagree',
        '5': 'Strongly Disagree'
    }

    def apply_conditional_likert_mapping(df, source_col, target_col, date_col, cutoff_date, std_map, rev_map):
        df[target_col] = df.apply(
            lambda row: (
                std_map.get(str(int(row[source_col]))) if pd.notnull(row[source_col]) and row[date_col] >= cutoff_date
                else rev_map.get(str(int(row[source_col]))) if pd.notnull(row[source_col])
                else "Null"
            ),
            axis=1
        )


    
    cutoff = pd.to_datetime("2024-01-09")
    df_clean["recorded_date"] = pd.to_datetime(df_clean["recorded_date"])

    conditional_agreement_fields = {
        "agree_content_useful_for_education": "agree_content_useful_for_education_label",
        "agree_content_relevant_to_career": "agree_content_relevant_to_career_label",
        "agree_workload_reasonable": "agree_workload_reasonable_label",
        "agree_deadlines_reasonable": "agree_deadlines_reasonable_label",
        "agree_content_relevant_to_personal_experience": "agree_content_relevant_to_personal_experience_label",
        "agree_assessments_alignment_with_course": "agree_assessments_alignment_with_course_label"
    }

    for source_col, target_col in conditional_agreement_fields.items():
        apply_conditional_likert_mapping(
            df_clean,
            source_col,
            target_col,
            date_col="recorded_date",
            cutoff_date=cutoff,
            std_map=agree_standard_map,
            rev_map=agree_reversed_map
        )



    return df_clean

# ────────────────────────
# Main Handler
# ────────────────────────

def run_qualtrics_pipeline():
    try:
        verify_authentication()
        progress_id = initiate_export(SURVEY_ID)
        file_id = wait_for_export(SURVEY_ID, progress_id)
        raw_df = download_responses(SURVEY_ID, file_id)
        final_df = clean_dataframe(raw_df, COLUMN_MAPPING)
        logging.info("✅ DataFrame ready for use.")
        print(final_df.head())
        print(f"Shape: {final_df.shape}")
        # print(final_df.columns.tolist())
        return final_df
    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}")
        raise


# ────────────────────────
# Entry Point
# ────────────────────────

if __name__ == "__main__":
    run_qualtrics_pipeline()