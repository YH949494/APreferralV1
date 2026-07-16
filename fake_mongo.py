"""Minimal in-memory Mongo-like fakes for unit-testing Campaign Centre logic
without a live MongoDB instance. Not a general-purpose mock — implements just
enough of find/find_one/find_one_and_update/insert_one/update_one/
count_documents for the write patterns used in this codebase.
"""

from __future__ import annotations

import itertools
from copy import deepcopy


class DuplicateKeyError(Exception):
    pass


def _get_dotted(doc: dict, dotted_key: str):
    cursor = doc
    for part in dotted_key.split("."):
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(part)
    return cursor


def _matches(doc: dict, query: dict) -> bool:
    for key, cond in query.items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue
        if key == "$and":
            if not all(_matches(doc, sub) for sub in cond):
                return False
            continue
        val = _get_dotted(doc, key)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, target in cond.items():
                if op == "$in" and val not in target:
                    return False
                if op == "$ne" and val == target:
                    return False
                if op == "$gt" and not (val is not None and val > target):
                    return False
                if op == "$gte" and not (val is not None and val >= target):
                    return False
                if op == "$exists" and (val is not None) != bool(target):
                    return False
        else:
            if val != cond:
                return False
    return True


def _set_dotted(doc: dict, dotted_key: str, value) -> None:
    parts = dotted_key.split(".")
    cursor = doc
    for part in parts[:-1]:
        cursor = cursor.setdefault(part, {})
    cursor[parts[-1]] = value


def _apply_update(doc: dict, update: dict, *, is_insert: bool = False) -> dict:
    new_doc = deepcopy(doc)
    if is_insert and "$setOnInsert" in update:
        for key, value in deepcopy(update["$setOnInsert"]).items():
            _set_dotted(new_doc, key, value)
    if "$set" in update:
        for key, value in deepcopy(update["$set"]).items():
            _set_dotted(new_doc, key, value)
    if "$inc" in update:
        for k, v in update["$inc"].items():
            new_doc[k] = new_doc.get(k, 0) + v
    return new_doc


class FakeCollection:
    def __init__(self, unique_keys: list[tuple] | None = None):
        self._docs: list[dict] = []
        self._id_counter = itertools.count(1)
        self._unique_keys = unique_keys or []

    def _next_id(self):
        return f"fakeid{next(self._id_counter)}"

    def _check_unique(self, doc: dict, exclude=None):
        for keyset in self._unique_keys:
            for existing in self._docs:
                if existing is exclude:
                    continue
                if all(existing.get(k) == doc.get(k) for k in keyset):
                    raise DuplicateKeyError(f"duplicate key on {keyset}")

    def insert_one(self, doc: dict):
        doc = deepcopy(doc)
        doc.setdefault("_id", self._next_id())
        self._check_unique(doc)
        self._docs.append(doc)
        return type("Result", (), {"inserted_id": doc["_id"]})()

    def find_one(self, query: dict | None = None, projection=None):
        query = query or {}
        for doc in self._docs:
            if _matches(doc, query):
                return deepcopy(doc)
        return None

    def find(self, query: dict | None = None, sort=None, limit=None, projection=None):
        query = query or {}
        results = [deepcopy(d) for d in self._docs if _matches(d, query)]
        if sort:
            for field, direction in reversed(sort):
                results.sort(key=lambda d, f=field: d.get(f) if d.get(f) is not None else 0,
                             reverse=(direction < 0))
        if limit:
            results = results[:limit]
        return results

    def count_documents(self, query: dict | None = None) -> int:
        return len(self.find(query))

    def update_one(self, query: dict, update: dict, upsert=False):
        for i, doc in enumerate(self._docs):
            if _matches(doc, query):
                self._docs[i] = _apply_update(doc, update)
                return type("Result", (), {"matched_count": 1, "modified_count": 1})()
        if upsert:
            base = {k: v for k, v in query.items() if not k.startswith("$")}
            new_doc = _apply_update(base, update, is_insert=True)
            new_doc.setdefault("_id", self._next_id())
            self._docs.append(new_doc)
            return type("Result", (), {"matched_count": 0, "modified_count": 0, "upserted_id": new_doc["_id"]})()
        return type("Result", (), {"matched_count": 0, "modified_count": 0})()

    def find_one_and_update(self, query: dict, update: dict, sort=None, return_document=None, **kwargs):
        candidates = [d for d in self._docs if _matches(d, query)]
        if sort:
            for field, direction in reversed(sort):
                candidates.sort(key=lambda d, f=field: d.get(f) if d.get(f) is not None else 0,
                                 reverse=(direction < 0))
        if not candidates:
            return None
        target = candidates[0]
        idx = self._docs.index(target)
        before = deepcopy(target)
        after = _apply_update(target, update)
        self._docs[idx] = after
        # ReturnDocument.AFTER == True-ish in pymongo's enum comparison used here;
        # tests always request AFTER semantics matching this codebase's usage.
        return deepcopy(after) if str(return_document).endswith("AFTER") or return_document else deepcopy(before)


class FakeDb:
    """Drop-in stand-in for ``database.db`` in tests: ``FakeDb()["collname"]``."""

    def __init__(self, unique_keys_by_collection: dict | None = None):
        self._collections: dict[str, FakeCollection] = {}
        self._unique_keys_by_collection = unique_keys_by_collection or {}

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self._collections:
            self._collections[name] = FakeCollection(self._unique_keys_by_collection.get(name))
        return self._collections[name]
