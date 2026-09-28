from src.silver import validation


# ---- required field ----


def test_check_required_field_passes_when_present_and_non_empty():
    result = validation.check_required_field({"lei": "ABC"}, "lei")
    assert result["passed"] is True
    assert result["severity"] == validation.HARD_FAIL


def test_check_required_field_fails_when_missing():
    result = validation.check_required_field({}, "lei")
    assert result["passed"] is False


def test_check_required_field_fails_when_blank():
    result = validation.check_required_field({"lei": "   "}, "lei")
    assert result["passed"] is False


# ---- optional field (WARNING severity) ----


def test_check_optional_field_present_true_but_still_warning_severity():
    result = validation.check_optional_field_present({"bic": "ABCDEF"}, "bic")
    assert result["passed"] is True
    assert result["severity"] == validation.WARNING


def test_check_optional_field_absent_is_warning_not_hard_fail():
    result = validation.check_optional_field_present({}, "bic")
    assert result["passed"] is False
    assert result["severity"] == validation.WARNING


# ---- pattern ----


def test_check_pattern_passes_for_matching_value():
    result = validation.check_pattern({"mcc_code": "0742"}, "mcc_code", r"^\d{4}$")
    assert result["passed"] is True


def test_check_pattern_fails_for_non_matching_value():
    result = validation.check_pattern({"mcc_code": "74"}, "mcc_code", r"^\d{4}$")
    assert result["passed"] is False


def test_check_pattern_fails_when_field_missing():
    result = validation.check_pattern({}, "mcc_code", r"^\d{4}$")
    assert result["passed"] is False


# ---- type ----


def test_check_type_passes_for_correct_type():
    assert validation.check_type({"lat": 53.8}, "lat", float)["passed"] is True


def test_check_type_fails_for_wrong_type():
    assert validation.check_type({"lat": "not-a-number"}, "lat", float)["passed"] is False


def test_check_type_none_value_passes_type_check():
    # A missing value is a required-field concern, not a type concern
    assert validation.check_type({"lat": None}, "lat", float)["passed"] is True


# ---- numeric range ----


def test_check_numeric_range_passes_within_bounds():
    result = validation.check_numeric_range({"lat": 53.8}, "lat", minimum=-90, maximum=90)
    assert result["passed"] is True


def test_check_numeric_range_fails_outside_bounds():
    result = validation.check_numeric_range({"lat": 200}, "lat", minimum=-90, maximum=90)
    assert result["passed"] is False


def test_check_numeric_range_boundary_values_pass():
    assert validation.check_numeric_range({"lat": 90}, "lat", minimum=-90, maximum=90)["passed"] is True
    assert validation.check_numeric_range({"lat": -90}, "lat", minimum=-90, maximum=90)["passed"] is True


def test_check_numeric_range_fails_for_non_numeric():
    result = validation.check_numeric_range({"lat": "x"}, "lat", minimum=-90, maximum=90)
    assert result["passed"] is False


# ---- positive (exchange_rate) ----


def test_check_positive_passes_for_positive_value():
    assert validation.check_positive({"exchange_rate": 1.08}, "exchange_rate")["passed"] is True


def test_check_positive_fails_for_zero():
    assert validation.check_positive({"exchange_rate": 0}, "exchange_rate")["passed"] is False


def test_check_positive_fails_for_negative():
    assert validation.check_positive({"exchange_rate": -1.5}, "exchange_rate")["passed"] is False


def test_check_positive_rejects_bool_even_though_bool_is_an_int_subclass():
    assert validation.check_positive({"exchange_rate": True}, "exchange_rate")["passed"] is False


# ---- uniqueness ----


def test_check_uniqueness_passes_when_no_duplicates():
    records = [{"id": 1}, {"id": 2}]
    result = validation.check_uniqueness(records, "id")
    assert result["passed"] is True
    assert result["details"]["duplicate_count"] == 0


def test_check_uniqueness_fails_when_duplicates_present():
    records = [{"id": 1}, {"id": 1}]
    result = validation.check_uniqueness(records, "id")
    assert result["passed"] is False
    assert result["details"]["duplicate_count"] == 1


# ---- referential integrity ----


def test_check_referential_integrity_null_value_is_not_a_violation():
    result = validation.check_referential_integrity({"currency_code": None}, "currency_code", {"USD", "GBP"})
    assert result["passed"] is True
    assert result["severity"] == validation.WARNING


def test_check_referential_integrity_matched_value_passes():
    result = validation.check_referential_integrity({"currency_code": "USD"}, "currency_code", {"USD", "GBP"})
    assert result["passed"] is True


def test_check_referential_integrity_unmatched_value_fails_as_warning():
    result = validation.check_referential_integrity({"currency_code": "ZZZ"}, "currency_code", {"USD", "GBP"})
    assert result["passed"] is False
    assert result["severity"] == validation.WARNING


# ---- classify ----


def test_classify_all_passing_is_accepted():
    checks = [
        validation.check_required_field({"lei": "ABC"}, "lei"),
        validation.check_optional_field_present({"bic": "X"}, "bic"),
    ]
    result = validation.classify(checks)
    assert result["accepted"] is True
    assert result["hard_failures"] == []
    assert result["warnings"] == []


def test_classify_hard_failure_rejects_record():
    checks = [validation.check_required_field({}, "lei")]
    result = validation.classify(checks)
    assert result["accepted"] is False
    assert len(result["hard_failures"]) == 1


def test_classify_only_warnings_still_accepted():
    checks = [
        validation.check_required_field({"lei": "ABC"}, "lei"),
        validation.check_optional_field_present({}, "bic"),
    ]
    result = validation.classify(checks)
    assert result["accepted"] is True
    assert len(result["warnings"]) == 1
    assert len(result["hard_failures"]) == 0


def test_classify_mixed_hard_failure_and_warning_still_rejects():
    checks = [
        validation.check_required_field({}, "lei"),
        validation.check_optional_field_present({}, "bic"),
    ]
    result = validation.classify(checks)
    assert result["accepted"] is False
    assert len(result["hard_failures"]) == 1
    assert len(result["warnings"]) == 1
