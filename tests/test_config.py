"""Tests for configuration and asset discovery."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest


class TestFindAssetFiles:

    def test_finds_brand_by_keyword(self, tmp_workspace):
        from linkedin_network_cleaner.core.config import find_asset_files
        assets_dir = tmp_workspace / "assets"
        (assets_dir / "my_brand_strategy.md").write_text("test brand")
        brand, persona = find_asset_files(assets_dir)
        assert brand is not None
        assert "brand" in brand.name.lower()

    def test_finds_persona_by_prefix(self, tmp_workspace):
        from linkedin_network_cleaner.core.config import find_asset_files
        assets_dir = tmp_workspace / "assets"
        (assets_dir / "Persona_ICP.md").write_text("test persona")
        brand, persona = find_asset_files(assets_dir)
        assert persona is not None

    def test_returns_none_when_missing(self, tmp_workspace):
        from linkedin_network_cleaner.core.config import find_asset_files
        assets_dir = tmp_workspace / "assets"
        brand, persona = find_asset_files(assets_dir)
        assert brand is None
        assert persona is None

    def test_finds_icp_keyword(self, tmp_workspace):
        from linkedin_network_cleaner.core.config import find_asset_files
        assets_dir = tmp_workspace / "assets"
        (assets_dir / "icp_definition.md").write_text("test icp")
        brand, persona = find_asset_files(assets_dir)
        assert persona is not None


class TestValidate:

    def test_validates_with_keys_set(self, monkeypatch):
        monkeypatch.setattr("linkedin_network_cleaner.core.config.API_KEY", "test_key")
        monkeypatch.setattr("linkedin_network_cleaner.core.config.IDENTITY_UUID", "test_uuid")
        from linkedin_network_cleaner.core.config import validate
        # Should not raise
        validate()

    def test_raises_on_missing_api_key(self, monkeypatch):
        monkeypatch.setattr("linkedin_network_cleaner.core.config.API_KEY", "")
        monkeypatch.setattr("linkedin_network_cleaner.core.config.IDENTITY_UUID", "test_uuid")
        from linkedin_network_cleaner.core.config import validate
        with pytest.raises(ValueError, match="EDGES_API_KEY"):
            validate()

    def test_raises_on_missing_identity(self, monkeypatch):
        monkeypatch.setattr("linkedin_network_cleaner.core.config.API_KEY", "test_key")
        monkeypatch.setattr("linkedin_network_cleaner.core.config.IDENTITY_UUID", "")
        from linkedin_network_cleaner.core.config import validate
        with pytest.raises(ValueError, match="EDGES_IDENTITY_UUID"):
            validate()


class TestLoadConfig:

    def test_returns_defaults_when_no_toml(self, tmp_workspace, monkeypatch):
        monkeypatch.setattr("linkedin_network_cleaner.core.config.WORKSPACE_DIR", tmp_workspace)
        from linkedin_network_cleaner.core.config import load_config
        cfg = load_config()
        assert cfg["extract"]["delay"] == 1.5
        assert cfg["clean"]["ai_threshold"] == 50

    def test_loads_toml_overrides(self, tmp_workspace, monkeypatch):
        monkeypatch.setattr("linkedin_network_cleaner.core.config.WORKSPACE_DIR", tmp_workspace)
        toml_content = '[clean]\nai_threshold = 60\nbatch_size = 10\n'
        (tmp_workspace / "linkedin-cleaner.toml").write_text(toml_content)
        from linkedin_network_cleaner.core.config import load_config
        cfg = load_config()
        assert cfg["clean"]["ai_threshold"] == 60
        assert cfg["clean"]["batch_size"] == 10
        # Other defaults preserved
        assert cfg["extract"]["delay"] == 1.5
