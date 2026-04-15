"""Shared test fixtures."""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def tmp_workspace(tmp_path):
    """Create a temporary workspace with standard directory structure."""
    for d in ["extracts", "analysis", "assets", "assets/Accounts", "assets/Prospects",
              "assets/Customers", "logs/actions", "logs/data"]:
        (tmp_path / d).mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def sample_master_df():
    """A small master DataFrame with varied profiles for testing decisions."""
    return pd.DataFrame([
        {
            "linkedin_profile_id": 1,
            "full_name": "Real Network Person",
            "real_network": True,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": None,
            "total_messages": 50,
            "their_messages": 25,
        },
        {
            "linkedin_profile_id": 2,
            "full_name": "Customer Employee",
            "real_network": False,
            "is_customer": True,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 20,
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 3,
            "full_name": "Former Customer",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": True,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 30,
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 4,
            "full_name": "Target Account Person",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": True,
            "is_target_prospect": False,
            "target_account_name": "Acme Corp",
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 15,
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 5,
            "full_name": "Engaged Follower",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 5,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 40,
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 6,
            "full_name": "High AI Score",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 75,
            "ai_icp_tag": "CHAMPION",
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 7,
            "full_name": "Threshold Boundary",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 49,
            "total_messages": 0,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 8,
            "full_name": "Has Conversation",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 20,
            "total_messages": 5,
            "their_messages": 3,
        },
        {
            "linkedin_profile_id": 9,
            "full_name": "Unanswered Outbound",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 10,
            "total_messages": 1,
            "their_messages": 0,
        },
        {
            "linkedin_profile_id": 10,
            "full_name": "Nobody Special",
            "real_network": False,
            "is_customer": False,
            "is_former_customer": False,
            "is_target_account": False,
            "is_target_prospect": False,
            "total_engagements": 0,
            "shared_school": False,
            "shared_experience": False,
            "ai_audience_fit": 10,
            "total_messages": 0,
            "their_messages": 0,
        },
    ])


@pytest.fixture
def sample_enrichment_data():
    """Sample enrichment data for testing."""
    return [
        {
            "linkedin_profile_id": 1,
            "full_name": "Test User",
            "current_job_title": "CEO",
            "current_company": "Test Corp",
            "experiences": [
                {"title": "CEO", "company_name": "Test Corp", "date_start": "2020-01", "date_end": ""},
            ],
            "skills": [{"name": "Python"}, {"name": "Sales"}],
            "summary": "A test user profile.",
        }
    ]
