from src.silver import quarantine


# ---- build_rejection ----


def test_build_rejection_contains_all_required_fields():
    rejection = quarantine.build_rejection(
        original_raw_record={"mcc": "abc"},
        error_reason="mcc_code did not match pattern ^\\d{4}$",
        failing_check_name="pattern_mcc_code",
        source_bronze_run_id="run_20260921T000000Z",
        source_record_identifier="abc",
    )
    assert rejection["original_raw_record"] == {"mcc": "abc"}
    assert rejection["error_reason"] == "mcc_code did not match pattern ^\\d{4}$"
    assert rejection["failing_check_name"] == "pattern_mcc_code"
    assert rejection["source_bronze_run_id"] == "run_20260921T000000Z"
    assert rejection["source_record_identifier"] == "abc"
    assert rejection["rejected_at_utc"]  # non-empty, generated


def test_build_rejection_preserves_original_record_exactly_even_when_nested():
    original = {"entity": {"jurisdiction": "GB-SCT"}, "lei": "X" * 20}
    rejection = quarantine.build_rejection(
        original_raw_record=original,
        error_reason="bad lei",
        failing_check_name="pattern_lei",
        source_bronze_run_id="run_1",
        source_record_identifier="X" * 20,
    )
    assert rejection["original_raw_record"] == original


# ---- write_quarantine ----


def test_write_quarantine_returns_none_and_creates_no_file_when_nothing_rejected(tmp_path):
    result = quarantine.write_quarantine("mcc", [], quarantine_root=tmp_path)
    assert result is None
    assert not any(tmp_path.iterdir())


def test_write_quarantine_creates_jsonl_file_with_rejected_records(tmp_path):
    rejections = [
        quarantine.build_rejection(
            original_raw_record={"mcc": "abc"},
            error_reason="bad pattern",
            failing_check_name="pattern_mcc_code",
            source_bronze_run_id="run_1",
            source_record_identifier="abc",
        )
    ]
    path = quarantine.write_quarantine("mcc", rejections, quarantine_root=tmp_path)

    assert path is not None
    assert path.exists()
    assert path.name == "rejected.jsonl"
    assert path.parent.parent.name == "mcc"


def test_write_quarantine_jsonl_format_one_record_per_line(tmp_path):
    rejections = [
        quarantine.build_rejection(
            original_raw_record={"id": 1}, error_reason="e1", failing_check_name="c1", source_bronze_run_id="run_1", source_record_identifier=1
        ),
        quarantine.build_rejection(
            original_raw_record={"id": 2}, error_reason="e2", failing_check_name="c2", source_bronze_run_id="run_1", source_record_identifier=2
        ),
    ]
    path = quarantine.write_quarantine("mcc", rejections, quarantine_root=tmp_path)

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2


def test_write_quarantine_and_read_quarantine_round_trip_preserves_all_records(tmp_path):
    rejections = [
        quarantine.build_rejection(
            original_raw_record={"id": i}, error_reason=f"e{i}", failing_check_name="c", source_bronze_run_id="run_1", source_record_identifier=i
        )
        for i in range(5)
    ]
    path = quarantine.write_quarantine("card_issuer", rejections, quarantine_root=tmp_path)

    read_back = quarantine.read_quarantine(path)
    assert len(read_back) == 5
    assert {r["original_raw_record"]["id"] for r in read_back} == {0, 1, 2, 3, 4}


def test_write_quarantine_multiple_runs_do_not_overwrite_each_other(tmp_path):
    rejection = quarantine.build_rejection(
        original_raw_record={"id": 1}, error_reason="e", failing_check_name="c", source_bronze_run_id="run_1", source_record_identifier=1
    )
    path1 = quarantine.write_quarantine("mcc", [rejection], quarantine_root=tmp_path)
    path2 = quarantine.write_quarantine("mcc", [rejection], quarantine_root=tmp_path)

    assert path1 != path2
    assert path1.exists()
    assert path2.exists()
