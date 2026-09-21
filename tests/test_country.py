from src.ingestion import country

SAMPLE_JSON = (
    '[{"name": {"common": "France"}, "cca2": "FR", "cca3": "FRA"},'
    ' {"name": {"common": "Germany"}, "cca2": "DE", "cca3": "DEU"}]'
)


def test_parse_for_metadata_returns_records_and_fields():
    records, fields = country.parse_for_metadata(SAMPLE_JSON)
    assert len(records) == 2
    assert "cca2" in fields


def test_parse_for_metadata_empty_list():
    records, fields = country.parse_for_metadata("[]")
    assert records == []
    assert fields == []


def test_parse_for_metadata_single_object_wrapped_in_list():
    records, _fields = country.parse_for_metadata('{"cca2": "FR"}')
    assert len(records) == 1
