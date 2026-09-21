from src.ingestion import mcc

SAMPLE_CSV = "mcc,edited_description\n5411,Grocery Stores\n5812,Eating Places\n"


def test_parse_for_metadata_returns_records_and_fields():
    records, fields = mcc.parse_for_metadata(SAMPLE_CSV)
    assert len(records) == 2
    assert "mcc" in fields
    assert records[0]["mcc"] == "5411"


def test_parse_for_metadata_empty_csv_returns_no_records():
    records, _fields = mcc.parse_for_metadata("mcc,edited_description\n")
    assert records == []


def test_parse_for_metadata_is_deterministic():
    first, _ = mcc.parse_for_metadata(SAMPLE_CSV)
    second, _ = mcc.parse_for_metadata(SAMPLE_CSV)
    assert first == second


def test_find_mcc_code_field_found():
    assert mcc._find_mcc_code_field(["mcc", "edited_description"]) == "mcc"


def test_find_mcc_code_field_not_found():
    assert mcc._find_mcc_code_field(["code", "description"]) is None


def test_parse_for_metadata_malformed_csv_does_not_crash():
    records, _fields = mcc.parse_for_metadata("not,a,proper\nheader")
    assert isinstance(records, list)
