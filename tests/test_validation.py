from src.ingestion import validation


def test_check_non_empty_true():
    assert validation.check_non_empty([{"a": 1}])["passed"] is True


def test_check_non_empty_false_on_empty_list():
    assert validation.check_non_empty([])["passed"] is False


def test_check_required_fields_all_present():
    records = [{"mcc": "5411"}, {"mcc": "5812"}]
    result = validation.check_required_fields(records, ["mcc"])
    assert result["passed"] is True


def test_check_required_fields_missing():
    records = [{"mcc": "5411"}, {"other": "x"}]
    result = validation.check_required_fields(records, ["mcc"])
    assert result["passed"] is False
    assert result["details"]["missing_counts"]["mcc"] == 1


def test_check_required_fields_no_records():
    result = validation.check_required_fields([], ["mcc"])
    assert result["passed"] is False


def test_check_duplicates_none():
    result = validation.check_duplicates([{"id": 1}, {"id": 2}], "id")
    assert result["passed"] is True
    assert result["details"]["duplicate_count"] == 0


def test_check_duplicates_found():
    result = validation.check_duplicates([{"id": 1}, {"id": 1}], "id")
    assert result["passed"] is False
    assert result["details"]["duplicate_count"] == 1


def test_check_nulls_counts_empty_and_none():
    records = [{"x": None}, {"x": ""}, {"x": "value"}]
    result = validation.check_nulls(records, ["x"])
    assert result["details"]["null_counts"]["x"] == 2


def test_combine_all_passed():
    checks = [{"name": "a", "passed": True}, {"name": "b", "passed": True}]
    assert validation.combine(checks)["passed"] is True


def test_combine_one_failed():
    checks = [{"name": "a", "passed": True}, {"name": "b", "passed": False}]
    assert validation.combine(checks)["passed"] is False


def test_check_duplicates_informational_does_not_fail():
    records = [{"code": "EUR"}, {"code": "EUR"}, {"code": "USD"}]
    result = validation.check_duplicates(records, "code", informational=True)
    assert result["passed"] is True
    assert result["details"]["duplicate_count"] == 1
    assert result["details"]["informational_only"] is True


def test_check_duplicates_non_informational_still_fails():
    records = [{"code": "EUR"}, {"code": "EUR"}]
    result = validation.check_duplicates(records, "code")
    assert result["passed"] is False


def test_check_field_pattern_all_match():
    records = [{"bin": "123456"}, {"bin": "654321"}]
    result = validation.check_field_pattern(records, "bin", r"^\d{6}$", "bin_6_digit")
    assert result["passed"] is True
    assert result["details"]["non_matching_count"] == 0


def test_check_field_pattern_reports_non_matching():
    records = [{"bin": "123456"}, {"bin": "12345"}, {"bin": "ABCDEF"}]
    result = validation.check_field_pattern(records, "bin", r"^\d{6}$", "bin_6_digit")
    assert result["passed"] is False
    assert result["details"]["non_matching_count"] == 2


def test_check_field_pattern_ignores_blank_values():
    records = [{"bin": "123456"}, {"bin": ""}]
    result = validation.check_field_pattern(records, "bin", r"^\d{6}$", "bin_6_digit")
    assert result["passed"] is True
