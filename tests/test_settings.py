import json
from pathlib import Path

from src.ingestion import settings as settings_module
from src.ingestion.settings import IngestionSettings, _get_int, _load_dotenv, settings


def test_get_int_returns_default_when_unset(monkeypatch):
    monkeypatch.delenv("SOME_UNSET_VAR", raising=False)
    assert _get_int("SOME_UNSET_VAR", 30) == 30


def test_get_int_parses_set_value(monkeypatch):
    monkeypatch.setenv("SOME_INT_VAR", "120")
    assert _get_int("SOME_INT_VAR", 30) == 120


def test_get_int_treats_empty_string_as_unset(monkeypatch):
    monkeypatch.setenv("SOME_INT_VAR", "")
    assert _get_int("SOME_INT_VAR", 30) == 30


def test_load_dotenv_sets_new_vars(tmp_path, monkeypatch):
    monkeypatch.delenv("FINPAY_TEST_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("FINPAY_TEST_KEY=hello\n", encoding="utf-8")
    _load_dotenv(env_file)
    assert settings_module.os.environ["FINPAY_TEST_KEY"] == "hello"
    monkeypatch.delenv("FINPAY_TEST_KEY", raising=False)


def test_load_dotenv_does_not_override_existing_env(tmp_path, monkeypatch):
    monkeypatch.setenv("FINPAY_TEST_KEY", "already_set")
    env_file = tmp_path / ".env"
    env_file.write_text("FINPAY_TEST_KEY=from_dotenv\n", encoding="utf-8")
    _load_dotenv(env_file)
    assert settings_module.os.environ["FINPAY_TEST_KEY"] == "already_set"


def test_load_dotenv_skips_comments_and_blank_lines(tmp_path, monkeypatch):
    monkeypatch.delenv("FINPAY_TEST_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("# a comment\n\nFINPAY_TEST_KEY=value\n", encoding="utf-8")
    _load_dotenv(env_file)
    assert settings_module.os.environ["FINPAY_TEST_KEY"] == "value"
    monkeypatch.delenv("FINPAY_TEST_KEY", raising=False)


def test_load_dotenv_missing_file_is_a_noop(tmp_path):
    _load_dotenv(tmp_path / "does_not_exist.env")  # must not raise


def test_settings_singleton_has_expected_shape():
    assert isinstance(settings.environment, str)
    assert isinstance(settings.request_timeout_seconds, int)
    assert isinstance(settings.user_agent, str)
    assert isinstance(settings.bronze_root, Path)
    assert isinstance(settings.log_level, str)
    assert settings.gleif_api_key is None or isinstance(settings.gleif_api_key, str)


def test_settings_is_frozen():
    import dataclasses

    import pytest

    with pytest.raises(dataclasses.FrozenInstanceError):
        settings.environment = "changed"


def test_no_fake_api_key_defaults():
    # None of the three live APIs require a key — the default must never be a
    # non-empty placeholder string that could be mistaken for a real credential.
    fresh = IngestionSettings()
    assert fresh.gleif_api_key is None


def test_get_logger_returns_configured_logger():
    from src.ingestion.settings import get_logger

    logger = get_logger("test.finpay.settings")
    assert logger.level != 0 or logger.getEffectiveLevel() != 0
    assert len(logger.handlers) >= 1


def test_sources_json_gleif_scope_is_locked_not_arbitrary():
    # Scope was left null as of Step 2 (deliberately undecided) and was later locked
    # by an explicit, evidence-based decision (Steps 6A-6E) — not silently guessed.
    config = json.loads(Path("configs/sources.json").read_text(encoding="utf-8"))
    gleif = config["gleif"]
    assert gleif["jurisdiction_filter"] == "GB"
    assert gleif["entity_status_filter"] == "ACTIVE"
    assert gleif["registration_status_filter"] == "ISSUED"
    assert gleif["sort"] == "lei"
    assert gleif["initial_load_max_records"] == 10000
    assert gleif["initial_load_max_pages"] == 50
    assert "sampling_limitation" in gleif  # the representativeness caveat must travel with the scope


def test_sources_json_is_valid_and_has_seven_sources():
    config = json.loads(Path("configs/sources.json").read_text(encoding="utf-8"))
    assert len(config) == 7
    for name, entry in config.items():
        assert "source_type" in entry, f"{name} missing source_type"
        assert entry["source_type"] in ("live_api", "reference")
