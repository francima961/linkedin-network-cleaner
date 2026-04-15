"""Tests for pipeline state management."""

import json
from pathlib import Path

import pandas as pd


def save_state(analysis_dir: Path, state: dict):
    """Save pipeline state."""
    path = analysis_dir / "pipeline_state.json"
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def load_state(analysis_dir: Path):
    """Load pipeline state."""
    path = analysis_dir / "pipeline_state.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def clear_state(analysis_dir: Path):
    """Clear pipeline state."""
    path = analysis_dir / "pipeline_state.json"
    if path.exists():
        path.unlink()


class TestPipelineState:

    def test_save_and_load(self, tmp_workspace):
        analysis_dir = tmp_workspace / "analysis"
        state = {"completed_steps": [1, 2, 3], "current_step": 4}
        save_state(analysis_dir, state)
        loaded = load_state(analysis_dir)
        assert loaded["completed_steps"] == [1, 2, 3]
        assert loaded["current_step"] == 4

    def test_load_missing_returns_none(self, tmp_workspace):
        analysis_dir = tmp_workspace / "analysis"
        assert load_state(analysis_dir) is None

    def test_clear_state(self, tmp_workspace):
        analysis_dir = tmp_workspace / "analysis"
        save_state(analysis_dir, {"completed_steps": [1]})
        clear_state(analysis_dir)
        assert load_state(analysis_dir) is None

    def test_parquet_snapshot_roundtrip(self, tmp_workspace):
        analysis_dir = tmp_workspace / "analysis"
        df = pd.DataFrame({"linkedin_profile_id": [1, 2, 3], "score": [80, 50, 20]})
        path = analysis_dir / "pipeline_step_1.parquet"
        df.to_parquet(path, index=False)
        loaded = pd.read_parquet(path)
        assert len(loaded) == 3
        assert list(loaded["score"]) == [80, 50, 20]
