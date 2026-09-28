import json
from datetime import datetime
from pathlib import Path

from src.silver import common, legal_entity, quarantine


def _make_record(
    lei,
    name=None,
    jurisdiction="GB",
    entity_status="ACTIVE",
    registration_status="ISSUED",
    last_update="2026-09-19T09:01:35Z",
    initial_reg="2015-01-01T00:00:00Z",
    next_renewal="2027-01-01T00:00:00Z",
    creation_date="2015-01-01T05:00:00Z",
    bic=None,
    legal_address=None,
    hq_address=None,
    legal_form_id="8888",
    category="GENERAL",
) -> dict:
    name = name or f"COMPANY {lei}"
    legal_address = (
        legal_address
        if legal_address is not None
        else {"addressLines": ["1 Test Street", "Suite 2"], "city": "London", "region": "GB-LND", "country": "GB", "postalCode": "E1 0AA"}
    )
    hq_address = hq_address if hq_address is not None else dict(legal_address)
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "legalAddress": legal_address,
                "headquartersAddress": hq_address,
                "jurisdiction": jurisdiction,
                "category": category,
                "legalForm": {"id": legal_form_id},
                "status": entity_status,
                "creationDate": creation_date,
            },
            "registration": {
                "initialRegistrationDate": initial_reg,
                "lastUpdateDate": last_update,
                "status": registration_status,
                "nextRenewalDate": next_renewal,
            },
            "bic": bic,
        },
    }


def _make_bronze_run(bronze_root: Path, run_id: str, pages: list[list[dict]], record_count: int | None = None) -> Path:
    run_dir = bronze_root / "gleif" / run_id
    run_dir.mkdir(parents=True)
    page_files = []
    total = 0
    for i, page_records in enumerate(pages, 1):
        filename = f"page_{i:04d}.json"
        content = {"meta": {"pagination": {"currentPage": i, "lastPage": len(pages)}}, "data": page_records}
        (run_dir / filename).write_text(json.dumps(content), encoding="utf-8")
        page_files.append(filename)
        total += len(page_records)
    metadata = {
        "raw_file": page_files[0] if page_files else "page_0001.json",
        "record_count": total if record_count is None else record_count,
        "ingestion_timestamp_utc": "2026-09-21T05:44:17Z",
        "page_files": page_files,
    }
    (run_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    return run_dir


def _run(bronze_root, silver_root, quarantine_root):
    return legal_entity.run(bronze_root=bronze_root, silver_root=silver_root, quarantine_root=quarantine_root)


LEI1 = "549300ABCDEFGHIJKL12"
LEI2 = "549300ABCDEFGHIJKL34"


# ---- transform: flattening, nested structures (JSON flattening, entity, registration, addresses) ----


def test_transform_extracts_lei_and_legal_name():
    record = legal_entity._transform_record(_make_record(LEI1, name="Test Legal Name"))
    assert record["lei"] == LEI1
    assert record["legal_name"] == "Test Legal Name"


def test_transform_extracts_nested_entity_fields():
    record = legal_entity._transform_record(_make_record(LEI1, entity_status="ACTIVE", category="GENERAL"))
    assert record["entity_status"] == "ACTIVE"
    assert record["entity_category"] == "GENERAL"
    assert record["legal_form_id"] == "8888"


def test_transform_extracts_nested_registration_fields():
    record = legal_entity._transform_record(_make_record(LEI1, registration_status="ISSUED"))
    assert record["registration_status"] == "ISSUED"
    assert record["registration_last_update_date"] == datetime(2026, 9, 19, 9, 1, 35)
    assert record["registration_initial_registration_date"] == datetime(2015, 1, 1)
    assert record["registration_next_renewal_date"] == datetime(2027, 1, 1)


def test_transform_legal_address_flattened_independently():
    el = _make_record(
        LEI1,
        legal_address={"addressLines": ["Legal Line"], "city": "Leeds", "region": "GB-WYK", "country": "gb", "postalCode": "LS1 1AA"},
        hq_address={"addressLines": ["HQ Line"], "city": "London", "region": "GB-LND", "country": "gb", "postalCode": "E1 0AA"},
    )
    record = legal_entity._transform_record(el)
    assert record["legal_address_city"] == "Leeds"
    assert record["legal_address_country"] == "GB"
    assert record["legal_address_lines"] == ["Legal Line"]
    assert record["hq_address_city"] == "London"
    assert record["hq_address_lines"] == ["HQ Line"]
    assert record["legal_address_city"] != record["hq_address_city"]


def test_transform_does_not_assume_legal_equals_hq_even_when_identical():
    el = _make_record(LEI1)  # helper defaults hq_address = copy of legal_address
    record = legal_entity._transform_record(el)
    assert record["legal_address_city"] == record["hq_address_city"]  # incidentally equal
    # but they are independently derived, not aliased
    assert record["legal_address_lines"] is not record["hq_address_lines"]


def test_transform_address_lines_empty_array_becomes_none():
    el = _make_record(LEI1, legal_address={"addressLines": [], "city": None, "region": None, "country": None, "postalCode": None})
    record = legal_entity._transform_record(el)
    assert record["legal_address_lines"] is None


# ---- required fields (missing LEI, malformed LEI, missing name/status/etc.) ----


def test_validate_rejects_missing_lei():
    el = _make_record(LEI1)
    el["attributes"]["lei"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_malformed_lei():
    record = legal_entity._transform_record(_make_record("TOO-SHORT"))
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_legal_name():
    el = _make_record(LEI1)
    el["attributes"]["entity"]["legalName"] = {}
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_entity_status():
    el = _make_record(LEI1)
    el["attributes"]["entity"]["status"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_registration_status():
    el = _make_record(LEI1)
    el["attributes"]["registration"]["status"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_jurisdiction():
    el = _make_record(LEI1)
    el["attributes"]["entity"]["jurisdiction"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_rejects_missing_last_update_date():
    el = _make_record(LEI1)
    el["attributes"]["registration"]["lastUpdateDate"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is False


def test_validate_accepts_well_formed_record():
    record = legal_entity._transform_record(_make_record(LEI1))
    assert legal_entity._validate_record(record)["accepted"] is True


# ---- optional fields become NULL, never invented (missing address/bic/legal_form/category/renewal) ----


def test_optional_missing_bic_becomes_none():
    record = legal_entity._transform_record(_make_record(LEI1, bic=None))
    assert record["bic"] is None


def test_optional_missing_legal_form_becomes_none():
    el = _make_record(LEI1)
    el["attributes"]["entity"]["legalForm"] = {}
    record = legal_entity._transform_record(el)
    assert record["legal_form_id"] is None


def test_optional_missing_category_becomes_none():
    el = _make_record(LEI1)
    el["attributes"]["entity"]["category"] = None
    record = legal_entity._transform_record(el)
    assert record["entity_category"] is None


def test_optional_missing_renewal_date_becomes_none():
    el = _make_record(LEI1)
    el["attributes"]["registration"]["nextRenewalDate"] = None
    record = legal_entity._transform_record(el)
    assert record["registration_next_renewal_date"] is None


def test_optional_missing_address_field_becomes_none_not_invented():
    el = _make_record(LEI1, legal_address={"addressLines": [], "city": None, "region": None, "country": None, "postalCode": None})
    record = legal_entity._transform_record(el)
    assert record["legal_address_city"] is None
    assert record["legal_address_country"] is None
    assert record["legal_address_postal_code"] != ""
    assert record["legal_address_postal_code"] is None


def test_optional_fields_missing_do_not_cause_rejection():
    el = _make_record(LEI1, bic=None, category=None)
    el["attributes"]["entity"]["legalForm"] = {}
    el["attributes"]["registration"]["nextRenewalDate"] = None
    record = legal_entity._transform_record(el)
    assert legal_entity._validate_record(record)["accepted"] is True


# ---- jurisdiction non-negotiable preservation ----


def test_jurisdiction_gb_preserved_exactly():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB"))
    assert record["entity_jurisdiction"] == "GB"


def test_jurisdiction_gb_sct_preserved_exactly():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB-SCT"))
    assert record["entity_jurisdiction"] == "GB-SCT"


def test_jurisdiction_gb_nir_preserved_exactly():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB-NIR"))
    assert record["entity_jurisdiction"] == "GB-NIR"


def test_jurisdiction_gb_sct_is_not_folded_to_gb():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB-SCT"))
    assert record["entity_jurisdiction"] != "GB"


def test_jurisdiction_gb_nir_is_not_folded_to_gb():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB-NIR"))
    assert record["entity_jurisdiction"] != "GB"


def test_jurisdiction_no_case_normalization_applied():
    # entity_jurisdiction must never be uppercased/altered, unlike country fields
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="GB-SCT"))
    assert record["entity_jurisdiction"] == "GB-SCT"  # not "GB-SCT".upper() coincidentally equal; proven via mixed case below


def test_jurisdiction_preserves_unexpected_mixed_case_value_verbatim():
    record = legal_entity._transform_record(_make_record(LEI1, jurisdiction="Gb-Sct"))
    assert record["entity_jurisdiction"] == "Gb-Sct"


# ---- end-to-end run(): multi-page flattening, quarantine, reconciliation, lineage ----


def test_run_reads_across_multiple_bronze_pages(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1)], [_make_record(LEI2)]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 2
    rows = common.read_parquet(result["silver_path"])
    assert {r["lei"] for r in rows} == {LEI1, LEI2}


def test_run_quarantines_malformed_lei(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    bad = _make_record("TOO-SHORT")
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1), bad]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["quarantined_count"] == 1
    assert result["inserted_count"] == 1
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert rejected[0]["source_record_identifier"] == "TOO-SHORT"
    assert rejected[0]["failing_check_name"] in {"pattern_lei", "required_field_lei"}


def test_run_reconciliation_success(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1), _make_record("TOO-SHORT")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    total = result["quarantined_count"] + result["inserted_count"] + result["updated_count"] + result["unchanged_count"] + result["stale_skipped_count"]
    assert total == result["bronze_record_count"]


def test_run_reconciliation_mismatch_raises_runtime_error(tmp_path):
    import pytest

    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1)]], record_count=99)  # lies about the count
    with pytest.raises(RuntimeError):
        _run(bronze_root, silver_root, quarantine_root)


def test_run_lineage_fields_present(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1)]])
    result = _run(bronze_root, silver_root, quarantine_root)
    rows = common.read_parquet(result["silver_path"])
    for field in common.STANDARD_LINEAGE_FIELDS:
        assert field in rows[0]
    assert rows[0]["source_name"] == "gleif"
    assert rows[0]["bronze_run_id"] == "run_1"
    assert "record_hash" in rows[0]
    assert rows[0]["source_updated_timestamp"] == rows[0]["registration_last_update_date"]


# ---- upsert semantics ----


def test_run_new_lei_is_inserted(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1)]])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI2)]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert {r["lei"] for r in rows} == {LEI1, LEI2}


def test_run_same_content_is_unchanged_no_op(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1)]])
    first = _run(bronze_root, silver_root, quarantine_root)
    before_rows = common.read_parquet(first["silver_path"])

    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI1)]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["unchanged_count"] == 1
    assert result["updated_count"] == 0
    after_rows = common.read_parquet(result["silver_path"])
    assert after_rows == before_rows  # untouched, including lineage


def test_run_newer_update_date_wins(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1, name="Old Name", last_update="2026-01-01T00:00:00Z")]])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI1, name="New Name", last_update="2026-06-01T00:00:00Z")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "New Name"


def test_run_older_update_date_does_not_overwrite(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1, name="Current Name", last_update="2026-06-01T00:00:00Z")]])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI1, name="Stale Name", last_update="2026-01-01T00:00:00Z")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["stale_skipped_count"] == 1
    assert result["updated_count"] == 0
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "Current Name"


def test_run_same_timestamp_different_content_is_quarantined_as_conflict(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1, name="Name A", last_update="2026-06-01T00:00:00Z")]])
    _run(bronze_root, silver_root, quarantine_root)

    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI1, name="Name B (conflict)", last_update="2026-06-01T00:00:00Z")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 0
    assert result["stale_skipped_count"] == 0
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "Name A"  # untouched


def test_run_missing_timestamp_ordering_is_quarantined_not_latest_wins(tmp_path):
    # Constructs the defensive branch directly: an existing Silver row with no
    # source_updated_timestamp (unreachable in practice since the field is
    # HARD_FAIL-required, but the code must still not invent latest-wins if it
    # ever occurs — e.g. a future schema/contract change).
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    silver_root.mkdir(parents=True)
    existing_row = {
        "lei": LEI1,
        "legal_name": "No Timestamp Existing",
        "legal_address_country": None,
        "legal_address_city": None,
        "legal_address_region": None,
        "legal_address_postal_code": None,
        "legal_address_lines": None,
        "hq_address_country": None,
        "hq_address_city": None,
        "hq_address_region": None,
        "hq_address_postal_code": None,
        "hq_address_lines": None,
        "legal_form_id": None,
        "entity_category": None,
        "entity_status": "ACTIVE",
        "entity_jurisdiction": "GB",
        "entity_creation_date": None,
        "registration_initial_registration_date": None,
        "registration_last_update_date": None,
        "registration_status": "ISSUED",
        "registration_next_renewal_date": None,
        "bic": None,
        "source_updated_timestamp": None,
        "record_hash": "deadbeef",
        "source_name": "gleif",
        "bronze_run_id": "run_0",
        "ingestion_timestamp_utc": "2026-01-01T00:00:00Z",
        "silver_processed_at_utc": "2026-01-01T00:00:00Z",
        "silver_transform_version": "v1",
    }
    common.write_parquet([existing_row], silver_root / "legal_entity" / "data.parquet", schema=legal_entity.LEGAL_ENTITY_SCHEMA)

    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1, name="Incoming Name", last_update="2026-06-01T00:00:00Z")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["updated_count"] == 0
    assert result["stale_skipped_count"] == 0
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)
    rows = common.read_parquet(result["silver_path"])
    assert rows[0]["legal_name"] == "No Timestamp Existing"  # not silently overwritten


# ---- idempotency, duplicate protection ----


def test_run_same_bronze_run_reprocessed_twice_is_idempotent(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1), _make_record(LEI2)]])
    _run(bronze_root, silver_root, quarantine_root)
    result2 = _run(bronze_root, silver_root, quarantine_root)

    assert result2["inserted_count"] == 0
    assert result2["unchanged_count"] == 2
    rows = common.read_parquet(result2["silver_path"])
    assert len(rows) == 2  # no duplicates


def test_run_no_duplicate_leis_across_runs(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    _make_bronze_run(bronze_root, "run_1", [[_make_record(LEI1, last_update="2026-01-01T00:00:00Z")]])
    _run(bronze_root, silver_root, quarantine_root)
    _make_bronze_run(bronze_root, "run_2", [[_make_record(LEI1, last_update="2026-06-01T00:00:00Z")]])
    result = _run(bronze_root, silver_root, quarantine_root)

    rows = common.read_parquet(result["silver_path"])
    leis = [r["lei"] for r in rows]
    assert len(leis) == len(set(leis))


def test_run_raises_file_not_found_when_no_bronze_run_exists(tmp_path):
    import pytest

    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    with pytest.raises(FileNotFoundError):
        legal_entity.run(bronze_root=bronze_root, silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")


# ---- record_hash determinism ----


def test_record_hash_deterministic_for_identical_content():
    record = legal_entity._transform_record(_make_record(LEI1))
    h1 = common.compute_record_hash(record, legal_entity.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(record, legal_entity.BUSINESS_HASH_FIELDS)
    assert h1 == h2


def test_record_hash_differs_for_different_content():
    r1 = legal_entity._transform_record(_make_record(LEI1, name="Name A"))
    r2 = legal_entity._transform_record(_make_record(LEI1, name="Name B"))
    h1 = common.compute_record_hash(r1, legal_entity.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(r2, legal_entity.BUSINESS_HASH_FIELDS)
    assert h1 != h2


def test_record_hash_unaffected_by_timestamp_or_lineage():
    r1 = legal_entity._transform_record(_make_record(LEI1, last_update="2026-01-01T00:00:00Z"))
    r2 = legal_entity._transform_record(_make_record(LEI1, last_update="2026-06-01T00:00:00Z"))
    h1 = common.compute_record_hash(r1, legal_entity.BUSINESS_HASH_FIELDS)
    h2 = common.compute_record_hash(r2, legal_entity.BUSINESS_HASH_FIELDS)
    assert h1 == h2  # timestamp excluded from the content hash by design


# ---- S7 regression: within-batch duplicate LEI must not silently last-write-wins
# overwrite (a real defect found and fixed in S7 — see src/silver/legal_entity.py's
# comment at the `existing = final_by_lei.get(key)` line; identical fix to merchant.py) ----


def test_run_within_batch_duplicate_lei_same_timestamp_is_quarantined_not_overwritten(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    version_a = _make_record(LEI1, name="Version A", last_update="2026-06-01T00:00:00Z")
    version_b = _make_record(LEI1, name="Version B", last_update="2026-06-01T00:00:00Z")
    _make_bronze_run(bronze_root, "run_1", [[version_a, version_b]])
    result = _run(bronze_root, silver_root, quarantine_root)

    # Before the fix, this silently produced inserted_count=2 and overwrote to
    # "Version B" via plain dict assignment, with reconciliation still passing
    # (2 records = 2 "inserted") — the bug was invisible to the count check.
    assert result["inserted_count"] == 1
    assert result["quarantined_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["legal_name"] == "Version A"  # first occurrence preserved, not silently overwritten
    rejected = quarantine.read_quarantine(result["quarantine_path"])
    assert any(r["failing_check_name"] == "upsert_conflict" for r in rejected)


def test_run_within_batch_duplicate_lei_prefers_newer_timestamp_not_array_order(tmp_path):
    bronze_root, silver_root, quarantine_root = tmp_path / "bronze", tmp_path / "silver", tmp_path / "quarantine"
    older_first = _make_record(LEI1, name="Older, First In Array", last_update="2026-01-01T00:00:00Z")
    newer_second = _make_record(LEI1, name="Newer, Second In Array", last_update="2026-06-01T00:00:00Z")
    _make_bronze_run(bronze_root, "run_1", [[older_first, newer_second]])
    result = _run(bronze_root, silver_root, quarantine_root)

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 1
    rows = common.read_parquet(result["silver_path"])
    assert len(rows) == 1
    assert rows[0]["legal_name"] == "Newer, Second In Array"


# ---- real local Bronze data ----


def test_run_against_real_local_bronze_data_reconciles(tmp_path):
    result = legal_entity.run(silver_root=tmp_path / "silver", quarantine_root=tmp_path / "quarantine")
    assert result["total_silver_rows"] > 0
    assert result["bronze_record_count"] > 0
