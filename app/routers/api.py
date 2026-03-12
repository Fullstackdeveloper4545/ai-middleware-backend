from __future__ import annotations

from datetime import datetime
import os
import threading
from typing import Any, Dict, Iterable, List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile

from ..auth import require_api_key
from ..database import (
    approvals_col,
    attribute_map_col,
    attribute_sessions_col,
    products_col,
    raw_imports_col,
    suppliers_col,
    sync_queue_col,
)
from ..schemas import (
    ApprovalRequest,
    ApprovalOut,
    AttributeRule,
    AttributeRuleOut,
    AttributeSessionOut,
    AttributeSessionUpdate,
    ImportCreateResponse,
    ImportOut,
    ProductCreate,
    ProductOut,
    ProductUpdate,
    SupplierCreate,
    SupplierOut,
    SupplierUpdate,
    SyncEnqueueRequest,
    SyncProcessRequest,
    SyncQueueOut,
)
from ..utils import (
    _normalize_attr_key,
    ai_extract,
    extract_description,
    extract_supplier_sku,
    extract_title,
    now_utc,
    parse_csv_bytes,
    score_confidence,
)

router = APIRouter(prefix="/api", tags=["api"], dependencies=[Depends(require_api_key)])


DEFAULT_ATTRIBUTES = [
    {"master_attribute": "Color", "allowed_values": [], "rules": None, "active": True},
    {"master_attribute": "Description", "allowed_values": [], "rules": None, "active": True},
    {"master_attribute": "Fabric", "allowed_values": [], "rules": None, "active": True},
    {"master_attribute": "SKU", "allowed_values": [], "rules": None, "active": True},
    {"master_attribute": "Material", "allowed_values": [], "rules": None, "active": False},
    {"master_attribute": "Size", "allowed_values": [], "rules": None, "active": False},
    {"master_attribute": "Style", "allowed_values": [], "rules": None, "active": False},
    {"master_attribute": "Drawers", "allowed_values": [], "rules": None, "active": False},
]


def _obj_id(value: str) -> ObjectId | str:
    if ObjectId.is_valid(value):
        return ObjectId(value)
    return value


def _serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(doc)
    if "_id" in data:
        data["_id"] = str(data["_id"])
    return data


def _serialize_many(cursor: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [_serialize(d) for d in cursor]


def _ensure_default_attributes() -> None:
    existing = attribute_map_col.find_one({})
    if existing:
        return
    for attr in DEFAULT_ATTRIBUTES:
        attribute_map_col.insert_one(
            {
                "master_attribute": attr["master_attribute"],
                "allowed_values": attr["allowed_values"],
                "rules": attr["rules"],
                "active": attr["active"],
            }
        )


def _active_attribute_names() -> List[str]:
    session = attribute_sessions_col.find_one({"is_active": True})
    if session and session.get("selected_attributes"):
        return list(session.get("selected_attributes") or [])
    _ensure_default_attributes()
    attrs = list(attribute_map_col.find({}))
    selected = [a.get("master_attribute", "") for a in attrs if a.get("active")]
    selected = [s for s in selected if s]
    if selected:
        return selected
    return [a.get("master_attribute", "") for a in attrs if a.get("master_attribute")]


def _available_attribute_names() -> List[str]:
    _ensure_default_attributes()
    attrs = list(attribute_map_col.find({}))
    return [a.get("master_attribute", "") for a in attrs if a.get("master_attribute")]


def _build_mapped_attributes(extracted: Dict[str, Any], active_attrs: List[str]) -> Dict[str, Any]:
    lower = {_normalize_attr_key(str(k)): v for k, v in extracted.items()}
    mapped: Dict[str, Any] = {}
    for attr in active_attrs:
        name = str(attr or "").strip()
        if not name:
            continue
        mapped[name] = lower.get(_normalize_attr_key(name), "")
    return mapped


def _get_or_create_active_session() -> Dict[str, Any]:
    session = attribute_sessions_col.find_one({"is_active": True})
    if session:
        return session
    selected = _active_attribute_names()
    available = _available_attribute_names()
    doc = {
        "selected_attributes": selected,
        "available_attributes": available or selected,
        "session_title": "Default Session",
        "is_active": True,
        "updated_at": now_utc(),
    }
    result = attribute_sessions_col.insert_one(doc)
    doc["_id"] = result.inserted_id
    return doc


@router.get("/suppliers", response_model=list[SupplierOut])
def list_suppliers():
    docs = list(suppliers_col.find({}))
    docs.sort(key=lambda d: d.get("created_at") or datetime.min, reverse=True)
    return _serialize_many(docs)


@router.post("/suppliers", response_model=SupplierOut)
def create_supplier(payload: SupplierCreate):
    doc = {
        "name": payload.name.strip(),
        "code": payload.code.strip(),
        "status": "active",
        "created_at": now_utc(),
    }
    res = suppliers_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    return _serialize(doc)


@router.patch("/suppliers/{supplier_id}", response_model=SupplierOut)
def update_supplier(supplier_id: str, payload: SupplierUpdate):
    update = {k: v for k, v in payload.dict().items() if v is not None}
    if not update:
        raise HTTPException(status_code=400, detail="Nothing to update")
    suppliers_col.update_one({"_id": _obj_id(supplier_id)}, {"$set": update})
    doc = suppliers_col.find_one({"_id": _obj_id(supplier_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Supplier not found")
    return _serialize(doc)


@router.delete("/suppliers/{supplier_id}")
def delete_supplier(supplier_id: str):
    suppliers_col.delete_one({"_id": _obj_id(supplier_id)})
    products_col.delete_many({"supplier_id": supplier_id})
    return {"ok": True}


@router.get("/attributes", response_model=list[AttributeRuleOut])
def list_attributes():
    _ensure_default_attributes()
    docs = list(attribute_map_col.find({}))
    docs.sort(key=lambda d: d.get("master_attribute", "").lower())
    return _serialize_many(docs)


@router.post("/attributes", response_model=AttributeRuleOut)
def upsert_attribute(payload: AttributeRule):
    _ensure_default_attributes()
    master = payload.master_attribute.strip()
    if not master:
        raise HTTPException(status_code=400, detail="master_attribute required")
    existing = attribute_map_col.find_one({"master_attribute": master})
    data = {
        "master_attribute": master,
        "allowed_values": payload.allowed_values or [],
        "rules": payload.rules,
    }
    if existing:
        attribute_map_col.update_one({"_id": existing["_id"]}, {"$set": data})
        doc = attribute_map_col.find_one({"_id": existing["_id"]})
    else:
        data["active"] = True
        res = attribute_map_col.insert_one(data)
        data["_id"] = res.inserted_id
        doc = data
    return _serialize(doc)


@router.get("/attributes/session", response_model=AttributeSessionOut)
def get_attribute_session():
    session = _get_or_create_active_session()
    return _serialize(session)


@router.post("/attributes/session", response_model=AttributeSessionOut)
def save_attribute_session(payload: AttributeSessionUpdate):
    selected = [s for s in (payload.selected_attributes or []) if str(s).strip()]
    available = [s for s in (payload.available_attributes or []) if str(s).strip()]
    session_title = (payload.session_title or "").strip() or None
    now = now_utc()
    if payload.session_id:
        session_id = payload.session_id
        attribute_sessions_col.update_one(
            {"_id": _obj_id(session_id)},
            {
                "$set": {
                    "selected_attributes": selected,
                    "available_attributes": available or selected,
                    "session_title": session_title,
                    "is_active": True,
                    "updated_at": now,
                }
            },
            upsert=True,
        )
        attribute_sessions_col.update_many(
            {"_id": {"$ne": _obj_id(session_id)}},
            {"$set": {"is_active": False}},
        )
        doc = attribute_sessions_col.find_one({"_id": _obj_id(session_id)})
        if doc:
            return _serialize(doc)
    doc = {
        "selected_attributes": selected,
        "available_attributes": available or selected,
        "session_title": session_title,
        "is_active": True,
        "updated_at": now,
    }
    res = attribute_sessions_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    attribute_sessions_col.update_many({"_id": {"$ne": res.inserted_id}}, {"$set": {"is_active": False}})
    return _serialize(doc)


@router.get("/attributes/sessions", response_model=list[AttributeSessionOut])
def list_attribute_sessions():
    docs = list(attribute_sessions_col.find({}))
    docs.sort(key=lambda d: d.get("updated_at") or datetime.min)
    return _serialize_many(docs)


@router.post("/attributes/sessions/{session_id}/activate", response_model=AttributeSessionOut)
def activate_attribute_session(session_id: str):
    doc = attribute_sessions_col.find_one({"_id": _obj_id(session_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Session not found")
    attribute_sessions_col.update_many({"_id": {"$ne": _obj_id(session_id)}}, {"$set": {"is_active": False}})
    attribute_sessions_col.update_one(
        {"_id": _obj_id(session_id)},
        {"$set": {"is_active": True, "updated_at": now_utc()}},
    )
    doc = attribute_sessions_col.find_one({"_id": _obj_id(session_id)})
    return _serialize(doc)


@router.delete("/attributes/sessions/{session_id}")
def delete_attribute_session(session_id: str):
    deleted = attribute_sessions_col.delete_one({"_id": _obj_id(session_id)})
    if deleted.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Session not found")
    # Ensure there is an active session
    active = attribute_sessions_col.find_one({"is_active": True})
    if not active:
        latest = attribute_sessions_col.find_one({}, sort=[("updated_at", -1)])
        if latest:
            attribute_sessions_col.update_one({"_id": latest["_id"]}, {"$set": {"is_active": True}})
    return {"ok": True}


@router.get("/imports", response_model=list[ImportOut])
def list_imports(supplier_id: Optional[str] = Query(default=None)):
    query: Dict[str, Any] = {}
    if supplier_id:
        query["supplier_id"] = supplier_id
    docs = list(raw_imports_col.find(query))
    docs.sort(key=lambda d: d.get("imported_at") or datetime.min, reverse=True)
    return _serialize_many(docs)


@router.post("/imports/csv", response_model=ImportCreateResponse)
async def upload_csv(
    supplier_id: str = Query(...),
    file: UploadFile = File(...),
):
    raw = await file.read()
    headers, rows = parse_csv_bytes(raw)
    now = now_utc()
    import_doc = {
        "supplier_id": supplier_id,
        "source_type": "csv",
        "source_ref": file.filename or "upload.csv",
        "total_rows": len(rows),
        "imported_at": now,
        "status": "processing",
        "error": None,
    }
    res = raw_imports_col.insert_one(import_doc)
    import_doc["_id"] = res.inserted_id

    replace_existing = os.getenv("IMPORT_REPLACE_SUPPLIER", "true").lower() in {"1", "true", "yes"}
    if replace_existing:
        old_ids = [str(p["_id"]) for p in products_col.find({"supplier_id": supplier_id})]
        products_col.delete_many({"supplier_id": supplier_id})
        if old_ids:
            approvals_col.delete_many({"product_id": {"$in": old_ids}})
            sync_queue_col.delete_many({"product_id": {"$in": old_ids}})

    active_attrs = _active_attribute_names()
    threading.Thread(
        target=_process_csv_import,
        args=(str(import_doc["_id"]), supplier_id, rows, active_attrs),
        daemon=True,
    ).start()

    return {
        "import_id": str(import_doc["_id"]),
        "supplier_id": supplier_id,
        "source_type": "csv",
        "source_ref": import_doc["source_ref"],
        "total_rows": len(rows),
        "created_at": now,
    }


def _process_csv_import(import_id: str, supplier_id: str, rows: List[Dict[str, Any]], active_attrs: List[str]) -> None:
    try:
        for row in rows:
            extracted = ai_extract(row, active_attrs)
            mapped = _build_mapped_attributes(extracted, active_attrs)
            product = {
                "supplier_id": supplier_id,
                "supplier_sku": extract_supplier_sku(row),
                "title": extract_title(row),
                "description": extract_description(row),
                "raw_attributes": row,
                "extracted_attributes": extracted,
                "mapped_attributes": mapped,
                "approval_status": "pending",
                "extraction_confidence": score_confidence(extracted),
                "created_at": now_utc(),
                "updated_at": now_utc(),
            }
            products_col.insert_one(product)
        raw_imports_col.update_one({"_id": _obj_id(import_id)}, {"$set": {"status": "completed"}})
    except Exception as exc:  # noqa: BLE001
        raw_imports_col.update_one(
            {"_id": _obj_id(import_id)},
            {"$set": {"status": "failed", "error": str(exc)}},
        )


@router.get("/products", response_model=list[ProductOut])
def list_products(
    supplier_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    query: Dict[str, Any] = {}
    if supplier_id:
        query["supplier_id"] = supplier_id
    if status:
        query["approval_status"] = status
    docs = list(products_col.find(query))
    docs.sort(key=lambda d: d.get("created_at") or datetime.min, reverse=True)
    return _serialize_many(docs)


@router.post("/products", response_model=ProductOut)
def create_product(payload: ProductCreate):
    now = now_utc()
    doc = {
        "supplier_id": payload.supplier_id,
        "supplier_sku": payload.supplier_sku,
        "title": payload.title,
        "description": payload.description,
        "raw_attributes": payload.raw_attributes or {},
        "extracted_attributes": payload.extracted_attributes or {},
        "mapped_attributes": payload.mapped_attributes or {},
        "approval_status": "pending",
        "extraction_confidence": 0.6,
        "created_at": now,
        "updated_at": now,
    }
    res = products_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    return _serialize(doc)


@router.patch("/products/{product_id}", response_model=ProductOut)
def update_product(product_id: str, payload: ProductUpdate):
    update = {k: v for k, v in payload.dict().items() if v is not None}
    if not update:
        raise HTTPException(status_code=400, detail="Nothing to update")
    update["updated_at"] = now_utc()
    products_col.update_one({"_id": _obj_id(product_id)}, {"$set": update})
    doc = products_col.find_one({"_id": _obj_id(product_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Product not found")
    return _serialize(doc)


@router.delete("/products/{product_id}")
def delete_product(product_id: str):
    products_col.delete_one({"_id": _obj_id(product_id)})
    approvals_col.delete_many({"product_id": product_id})
    sync_queue_col.delete_many({"product_id": product_id})
    return {"ok": True}


@router.post("/products/{product_id}/approve")
def approve_product(product_id: str, payload: ApprovalRequest):
    status = payload.status.lower()
    if status not in {"approved", "rejected"}:
        raise HTTPException(status_code=400, detail="Invalid status")
    now = now_utc()
    product = products_col.find_one({"_id": _obj_id(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    products_col.update_one(
        {"_id": _obj_id(product_id)},
        {"$set": {"approval_status": status, "updated_at": now}},
    )
    approvals_col.insert_one(
        {
            "product_id": product_id,
            "status": status,
            "reviewer": None,
            "notes": None,
            "updated_at": now,
        }
    )
    return {"ok": True}


@router.get("/approvals", response_model=list[ApprovalOut])
def list_approvals(product_id: Optional[str] = Query(default=None)):
    query: Dict[str, Any] = {}
    if product_id:
        query["product_id"] = product_id
    docs = list(approvals_col.find(query))
    docs.sort(key=lambda d: d.get("updated_at") or datetime.min, reverse=True)
    return _serialize_many(docs)


@router.get("/sync/queue", response_model=list[SyncQueueOut])
def list_sync_queue():
    docs = list(sync_queue_col.find({}))
    docs.sort(key=lambda d: d.get("created_at") or datetime.min, reverse=True)
    return _serialize_many(docs)


@router.post("/sync/enqueue")
def enqueue_sync(payload: SyncEnqueueRequest):
    query: Dict[str, Any] = {"approval_status": "approved"}
    if payload.supplier_id:
        query["supplier_id"] = payload.supplier_id
    products = list(products_col.find(query))
    now = now_utc()
    inserted = 0
    for p in products:
        product_id = str(p["_id"])
        existing = sync_queue_col.find_one({"product_id": product_id, "status": {"$in": ["queued", "processing"]}})
        if existing:
            continue
        sync_queue_col.insert_one(
            {
                "product_id": product_id,
                "supplier_id": p.get("supplier_id"),
                "status": "queued",
                "error": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        inserted += 1
    return {"queued": inserted}


@router.post("/sync/process")
def process_sync(payload: SyncProcessRequest):
    limit = max(1, payload.limit)
    docs = list(sync_queue_col.find({"status": "queued"}))
    docs.sort(key=lambda d: d.get("created_at") or datetime.min)
    now = now_utc()
    processed = 0
    for doc in docs[:limit]:
        sync_queue_col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": "synced", "updated_at": now}},
        )
        processed += 1
    return {"processed": processed}


@router.post("/seed")
def seed_demo():
    _ensure_default_attributes()
    if list(suppliers_col.find({})):
        return {"message": "Already seeded"}

    now = now_utc()
    supplier = {"name": "Demo Supplier", "code": "SUP-001", "status": "active", "created_at": now}
    res = suppliers_col.insert_one(supplier)
    supplier_id = str(res.inserted_id)

    session = {
        "selected_attributes": ["Color", "Description", "Fabric", "SKU"],
        "available_attributes": ["Color", "Description", "Fabric", "SKU", "Material", "Size"],
        "session_title": "Default Session",
        "is_active": True,
        "updated_at": now,
    }
    attribute_sessions_col.insert_one(session)

    products = [
        {
            "supplier_id": supplier_id,
            "supplier_sku": "SKU-001",
            "title": "Demo Velvet Headboard",
            "description": "Μαλακό κόκκινο βελούδο για κομψό υπνοδωμάτιο.",
            "raw_attributes": {},
            "extracted_attributes": {"color": "κόκκινο", "description": "Μαλακό κόκκινο βελούδο", "fabric": "βελούδο", "sku": "SKU-001"},
            "mapped_attributes": {"Color": "κόκκινο", "Description": "Μαλακό κόκκινο βελούδο", "Fabric": "βελούδο", "SKU": "SKU-001"},
            "approval_status": "pending",
            "extraction_confidence": 0.65,
            "created_at": now,
            "updated_at": now,
        },
        {
            "supplier_id": supplier_id,
            "supplier_sku": "SKU-002",
            "title": "Demo Headboard",
            "description": "Χρώμα: Σκούρο πράσινο, Ύφασμα: βελούδο.",
            "raw_attributes": {},
            "extracted_attributes": {"color": "Σκούρο πράσινο", "description": "Χρώμα: Σκούρο πράσινο", "fabric": "βελούδο", "sku": "SKU-002"},
            "mapped_attributes": {"Color": "Σκούρο πράσινο", "Description": "Χρώμα: Σκούρο πράσινο", "Fabric": "βελούδο", "SKU": "SKU-002"},
            "approval_status": "pending",
            "extraction_confidence": 0.65,
            "created_at": now,
            "updated_at": now,
        },
    ]
    for product in products:
        products_col.insert_one(product)

    return {"message": "Seeded demo data", "supplier_id": supplier_id}
