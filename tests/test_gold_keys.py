"""S14 — surrogate-key registry mechanics (src/gold/keys.py). Fully isolated
via tmp_path roots — never touches the real data/gold/_key_registry state.
"""

from src.gold.config import UNKNOWN_KEY, UNKNOWN_NATURAL_KEY
from src.gold.keys import assign_surrogate_keys, load_registry, save_registry, with_unknown_member


def test_assign_surrogate_keys_from_scratch_is_deterministic(tmp_path):
    reg1 = assign_surrogate_keys("dim_test", ["B", "A", "C"], root=tmp_path)
    # fresh registry each time -> same natural keys always map to the same ints
    reg2 = assign_surrogate_keys("dim_test", ["B", "A", "C"], root=tmp_path / "other")
    assert reg1 == reg2 == {"A": 1, "B": 2, "C": 3}


def test_assign_surrogate_keys_never_renumbers_existing(tmp_path):
    first = assign_surrogate_keys("dim_test", ["A", "B"], root=tmp_path)
    assert first == {"A": 1, "B": 2}
    second = assign_surrogate_keys("dim_test", ["A", "B", "C"], root=tmp_path)
    assert second["A"] == 1
    assert second["B"] == 2
    assert second["C"] == 3  # new key gets next-available, not renumbered from scratch


def test_assign_surrogate_keys_persists_across_calls(tmp_path):
    assign_surrogate_keys("dim_test", ["X"], root=tmp_path)
    reloaded = load_registry("dim_test", root=tmp_path)
    assert reloaded == {"X": 1}


def test_assign_surrogate_keys_deduplicates_repeated_natural_keys(tmp_path):
    reg = assign_surrogate_keys("dim_test", ["A", "A", "A", "B"], root=tmp_path)
    assert reg == {"A": 1, "B": 2}


def test_load_registry_returns_empty_dict_when_missing(tmp_path):
    assert load_registry("does_not_exist", root=tmp_path) == {}


def test_save_registry_roundtrip(tmp_path):
    path = save_registry("dim_test", {"A": 1, "B": 2}, root=tmp_path)
    assert path.exists()
    assert load_registry("dim_test", root=tmp_path) == {"A": 1, "B": 2}


def test_with_unknown_member_adds_reserved_key():
    reg = {"A": 1, "B": 2}
    with_unknown = with_unknown_member(reg)
    assert with_unknown[UNKNOWN_NATURAL_KEY] == UNKNOWN_KEY
    assert with_unknown["A"] == 1  # original entries untouched


def test_unknown_key_is_never_assigned_to_a_real_natural_key(tmp_path):
    reg = assign_surrogate_keys("dim_test", [f"K{i}" for i in range(50)], root=tmp_path)
    assert UNKNOWN_KEY not in reg.values()
