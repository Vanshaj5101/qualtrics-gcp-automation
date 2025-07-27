import pandas as pd
from typing import Dict
import numpy as np
from column_datatype_mapping import COLUMN_TYPE_MAPPING  # Import your mapping


def clean_dataframe(df: pd.DataFrame, col_mapping: Dict[str, str]) -> pd.DataFrame:

    # Step 1: Rename columns and drop top 2 rows
    df = df.rename(columns=col_mapping)
    df = df.iloc[2:].reset_index(drop=True)

     # Step 2: Enforce datatypes using COLUMN_TYPE_MAPPING
    # Step 2: Enforce datatypes using COLUMN_TYPE_MAPPING
    for col, dtype in COLUMN_TYPE_MAPPING.items():
        if col in df.columns:
            try:
                if "datetime" in str(dtype):
                    df[col] = pd.to_datetime(df[col], errors="coerce")

                elif dtype == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == str:
                    df[col] = df[col].astype(str)
                else:
                    df[col] = df[col].astype(dtype)

            except Exception as e:
                print(f"⚠️ Failed to cast column '{col}' to {dtype}: {e}")

    # Step 3: Label mapping
    satisfaction_map = {
        '1': 'Highly Dissatisfied', '2': 'Dissatisfied', '3': 'Neither Satisfied nor Dissatisfied',
        '4': 'Satisfied', '5': 'Highly Satisfied'
    }
    df["satisfaction_course_rating_label"] = df["satisfaction_course_rating"].apply(
        lambda x: satisfaction_map.get(str(int(x))) if pd.notnull(x) else np.nan
    )

    recommendation_map = {
        '1': 'Highly Unlikely', '2': 'Unlikely', '3': 'Neither Likely nor Unlikely',
        '4': 'Likely', '5': 'Highly Likely'
    }
    df["recommendation_rating_label"] = df["recommendation_rating"].apply(
        lambda x: recommendation_map.get(str(int(x))) if pd.notnull(x) else np.nan
    )

    usefulness_map = {
        '1': 'Not at all Useful', '2': 'Not very Useful', '3': 'Neutral',
        '4': 'Moderately Useful', '5': 'Extremely Useful'
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
    for src, tgt in usefulness_columns.items():
        df[tgt] = df[src].apply(
            lambda x: usefulness_map.get(str(int(x))) if pd.notnull(x) else np.nan
        )

    agree_standard = {
        '1': 'Strongly Disagree', '2': 'Disagree', '3': 'Neither Agree nor Disagree',
        '4': 'Agree', '5': 'Strongly Agree'
    }
    agree_reversed = {
        '1': 'Strongly Agree', '2': 'Agree', '3': 'Neither Agree nor Disagree',
        '4': 'Disagree', '5': 'Strongly Disagree'
    }

    cutoff = pd.to_datetime("2024-01-09")
    def conditional_likert(row, col):
        val = row[col]
        if pd.isnull(val):
            return np.nan
        return agree_standard[str(int(val))] if row["recorded_date"] >= cutoff else agree_reversed[str(int(val))]

    for col in [
        "agree_content_useful_for_education",
        "agree_content_relevant_to_career",
        "agree_workload_reasonable",
        "agree_deadlines_reasonable",
        "agree_content_relevant_to_personal_experience",
        "agree_assessments_alignment_with_course"
    ]:
        df[f"{col}_label"] = df.apply(lambda row: conditional_likert(row, col), axis=1)

    # Binary columns
    binary_map = {'1': 'Yes', '2': 'No'}
    df["willing_followup_call"] = df["willing_followup_call"].apply(
        lambda x: binary_map.get(str(int(x))) if pd.notnull(x) else np.nan
    )
    df["is_18_or_older"] = df["is_18_or_older"].apply(
        lambda x: binary_map.get(str(int(x))) if pd.notnull(x) else np.nan
    )

    return df