"""Worker startup must not scan or mutate earned XP history."""
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError

import xp


class IndexOnlyCollection:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def create_index(self, keys, **options):
        self.calls.append((keys, options))
        if self.error:
            raise self.error
        return options["name"]

    def aggregate(self, *args, **kwargs):
        raise AssertionError("startup attempted a full XP history scan")

    def delete_many(self, *args, **kwargs):
        raise AssertionError("startup attempted to delete earned XP")


def test_startup_keeps_uniqueness_without_history_scan(monkeypatch):
    events, ledger = IndexOnlyCollection(), IndexOnlyCollection()
    monkeypatch.setattr(xp, "_safe_create_index",
                        lambda collection, keys, **options:
                        collection.create_index(keys, **options))
    db = SimpleNamespace(xp_events=events, xp_ledger=ledger)
    # Worker restarts must remain safe as well as the first startup.
    for _ in range(2):
        xp.ensure_xp_indexes(db)
    unique = events.calls[0]
    assert unique[0] == [("user_id", 1), ("unique_key", 1)]
    assert unique[1]["unique"] is True
    assert unique[1]["partialFilterExpression"] == {"unique_key": {"$type": "string"}}
    assert ledger.calls[0][1]["unique"] is True


def test_duplicate_history_fails_index_build_without_deleting_records():
    error = DuplicateKeyError("duplicate existing XP data")
    db = SimpleNamespace(xp_events=IndexOnlyCollection(error),
                         xp_ledger=IndexOnlyCollection())
    with pytest.raises(DuplicateKeyError):
        xp.ensure_xp_indexes(db)
    assert not db.xp_ledger.calls
