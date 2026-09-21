from src.ingestion import iso_currency

SAMPLE_CSV = (
    "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n"
    "AFGHANISTAN,Afghani,AFN,971,2,\n"
    "ALAND ISLANDS,Euro,EUR,978,2,\n"
    "FRANCE,Euro,EUR,978,2,\n"  # real behavior: AlphabeticCode repeats across entities
    "SOME FUND,Special Drawing Rights,XDR,960,,\n"  # real behavior: MinorUnit can be blank
)


def test_parse_for_metadata_returns_records_and_fields():
    records, fields = iso_currency.parse_for_metadata(SAMPLE_CSV)
    assert len(records) == 4
    assert "AlphabeticCode" in fields
    assert "MinorUnit" in fields


def test_parse_for_metadata_preserves_expected_columns():
    _records, fields = iso_currency.parse_for_metadata(SAMPLE_CSV)
    expected = {"Entity", "Currency", "AlphabeticCode", "NumericCode", "MinorUnit", "WithdrawalDate"}
    assert expected.issubset(set(fields))


def test_missing_required_columns_detected_by_validation():
    from src.ingestion import validation

    broken_csv = "Entity,Currency\nFRANCE,Euro\n"
    records, _fields = iso_currency.parse_for_metadata(broken_csv)
    result = validation.check_required_fields(records, ["AlphabeticCode", "Currency", "MinorUnit"])
    assert result["passed"] is False
    assert "AlphabeticCode" in result["details"]["missing_counts"]


def test_duplicate_alphabetic_code_is_informational_not_a_failure():
    from src.ingestion import validation

    records, _fields = iso_currency.parse_for_metadata(SAMPLE_CSV)
    result = validation.check_duplicates(records, "AlphabeticCode", informational=True)
    assert result["details"]["duplicate_count"] == 1  # EUR appears twice
    assert result["passed"] is True  # must not fail — this is expected source behavior


def test_minor_unit_blank_value_is_preserved_not_coerced():
    records, _fields = iso_currency.parse_for_metadata(SAMPLE_CSV)
    xdr_row = next(r for r in records if r["AlphabeticCode"] == "XDR")
    assert xdr_row["MinorUnit"] == ""  # preserved as empty string, not silently "0"


def test_parse_for_metadata_empty_dataset():
    records, _fields = iso_currency.parse_for_metadata(
        "Entity,Currency,AlphabeticCode,NumericCode,MinorUnit,WithdrawalDate\n"
    )
    assert records == []


def test_parse_for_metadata_malformed_csv_does_not_crash():
    records, _fields = iso_currency.parse_for_metadata("not,a,proper\nheader")
    assert isinstance(records, list)
