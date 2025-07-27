from typing import Dict, Any

COLUMN_TYPE_MAPPING: Dict[str, Any] = {
    "start_date": "datetime64[ns]",
    "end_date": "datetime64[ns]",
    "response_status": "Int64",
    "ip_address": str,
    "progress_percent": "Int64",
    "duration_seconds": "Int64",
    "is_finished": "Int64",
    "recorded_date": "datetime64[ns]",
    "response_id": str,
    "recipient_last_name": str,
    "recipient_first_name": str,
    "recipient_email": str,
    "external_reference": str,
    "location_latitude": str,
    "location_longitude": str,
    "distribution_channel": str,
    "user_language": str,
    
    "reason_for_course_selection_college_credits": "Int64",
    "reason_for_course_selection_specific_credits": "Int64",
    "reason_for_course_selection_career_prospects": "Int64",
    "reason_for_course_selection_pathway": "Int64",
    "reason_for_course_selection_interest": "Int64",
    "reason_for_course_selection_other": "Int64",
    "reason_for_course_selection_other_text": str,
    
    "satisfaction_course_rating": "Int64",
    "recommendation_rating": "Int64",
    "satisfaction_course_rating_text": str,
    
    "challenge_academic_support": "Int64",
    "challenge_understanding_content": "Int64",
    "challenge_learning_tools": "Int64",
    "challenge_understanding_grades": "Int64",
    "challenge_navigating_canvas": "Int64",
    "challenge_getting_feedback": "Int64",
    "challenge_technical_requirements_proctoring_exams": "Int64",
    "challenge_other": "Int64",
    "challenge_other_text": str,
    
    "agree_content_useful_for_education": "Int64",
    "agree_content_relevant_to_career": "Int64",
    "agree_workload_reasonable": "Int64",
    "agree_deadlines_reasonable": "Int64",
    "agree_content_relevant_to_personal_experience": "Int64",
    "agree_assessments_alignment_with_course": "Int64",
    
    "useful_video_lectures": "Int64",
    "useful_reading_materials": "Int64",
    "useful_discussion_boards": "Int64",
    "useful_interactive_tools": "Int64",
    "useful_projects": "Int64",
    "useful_reflection_journaling": "Int64",
    "useful_engagement": "Int64",
    
    "course_feedback_liked_best": str,
    "course_feedback_improvement_suggestions_text": str,
    
    "willing_followup_call": "Int64",
    "is_18_or_older": "Int64",
    
    "followup_email": str,
    "user_id": str,
    "course_id": str,
    "outcomes_id": str
}
