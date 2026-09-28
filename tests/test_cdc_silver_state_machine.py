"""S13 — unit tests for src/cdc/silver.py's current-state event state
machine (_apply_current_state_event), the core out-of-order/stale/delete
logic (§14). Pure function, no Bronze/Postgres needed.
"""

from src.cdc.silver import TableRunResult, _apply_current_state_event


def _rec(amount="1"):
    return {"amount": amount}


def test_first_event_for_a_key_is_inserted():
    key_state, result = {}, TableRunResult(dataset="x")
    disp = _apply_current_state_event(key_state, "K1", "c", 100, _rec(), "hashA", result)
    assert disp == "inserted"
    assert result.inserted == 1
    assert key_state["K1"].record == _rec()


def test_newer_event_with_different_content_updates():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    disp = _apply_current_state_event(key_state, "K1", "u", 200, _rec("2"), "hashB", result)
    assert disp == "updated"
    assert result.updated == 1
    assert key_state["K1"].record == _rec("2")
    assert key_state["K1"].lsn == 200


def test_newer_event_with_same_content_is_unchanged_and_leaves_state_untouched():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    disp = _apply_current_state_event(key_state, "K1", "u", 200, _rec("1"), "hashA", result)
    assert disp == "unchanged"
    assert result.unchanged == 1
    # existing row is untouched — still shows the ORIGINAL lsn (100), not
    # advanced to 200, matching src/silver/merchant.py's exact convention.
    assert key_state["K1"].lsn == 100


def test_older_event_never_overwrites_newer_state():
    """INSERT -> UPDATE(newer) -> UPDATE(older): the older one must not win,
    per §14's explicit worked scenario."""
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    _apply_current_state_event(key_state, "K1", "u", 300, _rec("3"), "hashC", result)  # newer, applied
    disp = _apply_current_state_event(key_state, "K1", "u", 200, _rec("2"), "hashB", result)  # older, arrives late
    assert disp == "stale_skipped"
    assert result.stale_skipped == 1
    assert key_state["K1"].record == _rec("3")  # newer state preserved


def test_delete_removes_the_key():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec(), "hashA", result)
    disp = _apply_current_state_event(key_state, "K1", "d", 200, None, None, result)
    assert disp == "deleted"
    assert result.deleted == 1
    assert key_state["K1"].deleted is True
    assert key_state["K1"].record is None


def test_delete_older_than_current_state_is_stale_not_applied():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec(), "hashA", result)
    _apply_current_state_event(key_state, "K1", "u", 300, _rec("3"), "hashC", result)
    disp = _apply_current_state_event(key_state, "K1", "d", 200, None, None, result)  # older than 300
    assert disp == "stale_skipped"
    assert key_state["K1"].deleted is False  # newer state survives, not deleted
    assert key_state["K1"].record == _rec("3")


def test_insert_after_delete_is_a_fresh_insert():
    """INSERT -> DELETE -> (later) new INSERT with the same PK: a legitimate
    re-creation, not a resurrection of stale data — accepted."""
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    _apply_current_state_event(key_state, "K1", "d", 200, None, None, result)
    disp = _apply_current_state_event(key_state, "K1", "c", 300, _rec("new"), "hashNew", result)
    assert disp == "inserted"
    assert key_state["K1"].deleted is False
    assert key_state["K1"].record == _rec("new")
    assert result.inserted == 2  # both the original and the re-creation


def test_update_against_a_deleted_key_is_a_conflict_never_silently_resurrected():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    _apply_current_state_event(key_state, "K1", "d", 200, None, None, result)
    disp = _apply_current_state_event(key_state, "K1", "u", 300, _rec("resurrect"), "hashR", result)
    assert disp == "conflict_update_after_delete"
    assert key_state["K1"].deleted is True  # never resurrected


def test_stale_update_against_a_deleted_key_is_ignored():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    _apply_current_state_event(key_state, "K1", "d", 300, None, None, result)
    disp = _apply_current_state_event(key_state, "K1", "u", 150, _rec("late"), "hashL", result)  # older than the delete
    assert disp == "stale_skipped"
    assert key_state["K1"].deleted is True


def test_same_lsn_different_content_is_a_conflict():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    disp = _apply_current_state_event(key_state, "K1", "u", 100, _rec("2"), "hashB", result)
    assert disp == "conflict_same_lsn"
    assert key_state["K1"].record == _rec("1")  # untouched


def test_same_lsn_same_content_is_unchanged():
    key_state, result = {}, TableRunResult(dataset="x")
    _apply_current_state_event(key_state, "K1", "c", 100, _rec("1"), "hashA", result)
    disp = _apply_current_state_event(key_state, "K1", "u", 100, _rec("1"), "hashA", result)
    assert disp == "unchanged"


def test_delete_for_a_never_seen_key_still_recorded_as_deleted():
    # e.g. a delete event whose insert predates this Silver dataset's history
    key_state, result = {}, TableRunResult(dataset="x")
    disp = _apply_current_state_event(key_state, "K1", "d", 100, None, None, result)
    assert disp == "deleted"
    assert key_state["K1"].deleted is True
