import atexit
import logging
import os
import time
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from .config import MONGODB_URI, DB_NAME

logger = logging.getLogger(__name__)


class InMemoryUpdateResult:
    def __init__(self, matched_count: int, upserted_id: Optional[ObjectId]):
        self.matched_count = matched_count
        self.upserted_id = upserted_id


class InMemoryInsertOneResult:
    def __init__(self, inserted_id: ObjectId):
        self.inserted_id = inserted_id


class InMemoryDeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class InMemoryDeleteManyResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class InMemoryUpdateManyResult:
    def __init__(self, matched_count: int):
        self.matched_count = matched_count


class InMemoryCursor:
    def __init__(self, docs: List[Dict[str, Any]]):
        self._docs = docs

    def sort(self, key: str, direction: int = 1):
        reverse = direction == -1
        self._docs.sort(key=lambda d: d.get(key), reverse=reverse)
        return self

    def limit(self, n: int):
        self._docs = self._docs[:n]
        return self

    def __iter__(self) -> Iterable[Dict[str, Any]]:
        return iter(self._docs)


class InMemoryCollection:
    def __init__(self):
        self._docs: List[Dict[str, Any]] = []

    def create_index(self, *args, **kwargs):
        return None

    def find(self, query: Optional[Dict[str, Any]] = None) -> InMemoryCursor:
        query = query or {}
        results = [d for d in self._docs if _match_query(d, query)]
        return InMemoryCursor(deepcopy(results))

    def find_one(self, query: Dict[str, Any], sort: Optional[List[tuple[str, int]]] = None) -> Optional[Dict[str, Any]]:
        results = [d for d in self._docs if _match_query(d, query)]
        if sort:
            key, direction = sort[0]
            reverse = direction == -1
            results.sort(key=lambda d: d.get(key), reverse=reverse)
        return deepcopy(results[0]) if results else None

    def insert_one(self, doc: Dict[str, Any]) -> InMemoryInsertOneResult:
        new_doc = deepcopy(doc)
        new_doc.setdefault("_id", ObjectId())
        self._docs.append(new_doc)
        return InMemoryInsertOneResult(new_doc["_id"])

    def update_one(self, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> InMemoryUpdateResult:
        for idx, d in enumerate(self._docs):
            if _match_query(d, query):
                if "$set" in update:
                    self._docs[idx] = {**d, **deepcopy(update["$set"])}
                return InMemoryUpdateResult(matched_count=1, upserted_id=None)

        if upsert:
            new_doc = deepcopy(query)
            if "$set" in update:
                new_doc.update(deepcopy(update["$set"]))
            new_doc.setdefault("_id", ObjectId())
            self._docs.append(new_doc)
            return InMemoryUpdateResult(matched_count=0, upserted_id=new_doc["_id"])

        return InMemoryUpdateResult(matched_count=0, upserted_id=None)

    def update_many(self, query: Dict[str, Any], update: Dict[str, Any]) -> InMemoryUpdateManyResult:
        matched = 0
        if "$set" not in update:
            return InMemoryUpdateManyResult(matched_count=0)
        for idx, d in enumerate(self._docs):
            if _match_query(d, query):
                self._docs[idx] = {**d, **deepcopy(update["$set"])}
                matched += 1
        return InMemoryUpdateManyResult(matched_count=matched)

    def delete_one(self, query: Dict[str, Any]) -> InMemoryDeleteResult:
        for idx, d in enumerate(self._docs):
            if _match_query(d, query):
                del self._docs[idx]
                return InMemoryDeleteResult(deleted_count=1)
        return InMemoryDeleteResult(deleted_count=0)

    def delete_many(self, query: Dict[str, Any]) -> InMemoryDeleteManyResult:
        original = len(self._docs)
        self._docs = [d for d in self._docs if not _match_query(d, query)]
        return InMemoryDeleteManyResult(deleted_count=original - len(self._docs))


def _match_query(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
    for key, value in query.items():
        if isinstance(value, dict):
            if "$in" in value:
                if doc.get(key) not in value["$in"]:
                    return False
                continue
            if "$ne" in value:
                if doc.get(key) == value["$ne"]:
                    return False
                continue
        else:
            if doc.get(key) != value:
                return False
    return True


def _connect_mongo(retries: int = 5, delay_seconds: float = 1.0):
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            logger.info("MongoDB connected (attempt %s/%s).", attempt, retries)
            return client, client[DB_NAME]
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("MongoDB connection failed (attempt %s/%s): %s", attempt, retries, exc)
            time.sleep(delay_seconds)
    raise RuntimeError(f"MongoDB unavailable after {retries} attempts: {last_exc}")


USE_INMEMORY = os.getenv("USE_INMEMORY_DB", "").lower() in {"1", "true", "yes"}

client: Optional[MongoClient] = None
db: Any

if not USE_INMEMORY:
    try:
        client, mongo_db = _connect_mongo()
        db = mongo_db

        def _close_client():
            try:
                if client:
                    client.close()
            except Exception:
                pass

        atexit.register(_close_client)
    except (PyMongoError, RuntimeError) as exc:
        logger.warning("MongoDB unavailable, falling back to in-memory DB: %s", exc)
        USE_INMEMORY = True

if USE_INMEMORY:
    db = {
        "suppliers": InMemoryCollection(),
        "products": InMemoryCollection(),
        "raw_imports": InMemoryCollection(),
        "attribute_map": InMemoryCollection(),
        "attribute_sessions": InMemoryCollection(),
        "approvals": InMemoryCollection(),
        "sync_queue": InMemoryCollection(),
    }


def _col(name: str):
    return db[name] if isinstance(db, dict) else db[name]


suppliers_col = _col("suppliers")
products_col = _col("products")
raw_imports_col = _col("raw_imports")
attribute_map_col = _col("attribute_map")
attribute_sessions_col = _col("attribute_sessions")
approvals_col = _col("approvals")
sync_queue_col = _col("sync_queue")
