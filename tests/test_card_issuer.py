from src.ingestion import card_issuer, validation

SAMPLE_CSV = (
    "BIN,Brand,Type,Category,Issuer,IssuerPhone,IssuerUrl,isoCode2,isoCode3,CountryName\n"
    '002102,"PRIVATE LABEL",CREDIT,STANDARD,"CHINA MERCHANTS BANK",95555,https://example.com,CN,CHN,CHINA\n'
    "010083,PAGOBANCOMAT,DEBIT,,,,,IT,ITA,ITALY\n"  # real behavior: Issuer can be blank
    "020202,VISA,CREDIT,GOLD,SOME BANK,,,US,USA,UNITED STATES\n"
)


def test_parse_for_metadata_returns_records_and_fields():
    records, fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    assert len(records) == 3
    assert "BIN" in fields
    assert "Issuer" in fields


def test_expected_columns_present():
    _records, fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    expected = {
        "BIN", "Brand", "Type", "Category", "Issuer",
        "IssuerPhone", "IssuerUrl", "isoCode2", "isoCode3", "CountryName",
    }
    assert expected.issubset(set(fields))


def test_bin_is_treated_as_string_with_leading_zero_preserved():
    records, _fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    first_bin = records[0]["BIN"]
    assert first_bin == "002102"
    assert isinstance(first_bin, str)
    assert first_bin.startswith("0")


def test_bin_digit_length_validation_against_observed_6_digit_pattern():
    records, _fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    result = validation.check_field_pattern(records, "BIN", card_issuer.BIN_PATTERN, "bin_6_digit")
    assert result["passed"] is True


def test_bin_digit_length_validation_flags_wrong_length():
    records = [{"BIN": "12345"}, {"BIN": "1234567"}]  # 5 and 7 digits — should be flagged
    result = validation.check_field_pattern(records, "BIN", card_issuer.BIN_PATTERN, "bin_6_digit")
    assert result["passed"] is False
    assert result["details"]["non_matching_count"] == 2


def test_invalid_iso_country_code_flagged():
    records = [{"isoCode2": "usa"}, {"isoCode2": "GB"}]  # lowercase should fail the pattern
    result = validation.check_field_pattern(records, "isoCode2", card_issuer.ISO2_PATTERN, "iso_alpha2_country")
    assert result["passed"] is False
    assert result["details"]["non_matching_count"] == 1


def test_duplicate_bin_handling():
    records, _fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    result = validation.check_duplicates(records, "BIN")
    assert result["passed"] is True
    assert result["details"]["duplicate_count"] == 0


def test_missing_issuer_does_not_fail_required_fields_check():
    records, _fields = card_issuer.parse_for_metadata(SAMPLE_CSV)
    result = validation.check_required_fields(records, ["BIN", "Brand"])
    assert result["passed"] is True  # Issuer is deliberately not required


def test_parse_for_metadata_empty_dataset():
    records, _fields = card_issuer.parse_for_metadata(
        "BIN,Brand,Type,Category,Issuer,IssuerPhone,IssuerUrl,isoCode2,isoCode3,CountryName\n"
    )
    assert records == []


def test_parse_for_metadata_malformed_csv_does_not_crash():
    records, _fields = card_issuer.parse_for_metadata("not,a,proper\nheader")
    assert isinstance(records, list)
