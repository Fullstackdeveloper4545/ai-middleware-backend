from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ImportCreateResponse(BaseModel):
    import_id: str
    supplier_id: str
    source_type: str
    source_ref: str
    total_rows: int
    created_at: datetime


class ImportOut(BaseModel):
    id: str = Field(alias="_id")
    supplier_id: str
    source_type: str
    source_ref: str
    total_rows: int
    imported_at: datetime
    status: Optional[str] = None
    error: Optional[str] = None


class SupplierCreate(BaseModel):
    name: str
    code: str


class SupplierOut(BaseModel):
    id: str = Field(alias="_id")
    name: str
    code: str
    status: str
    created_at: datetime


class SupplierUpdate(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    status: Optional[str] = None


class AttributeRule(BaseModel):
    master_attribute: str
    allowed_values: List[str]
    rules: Optional[str] = None


class AttributeRuleOut(AttributeRule):
    id: str = Field(alias="_id")
    active: Optional[bool] = None


class AttributeSessionUpdate(BaseModel):
    selected_attributes: List[str]
    available_attributes: List[str] | None = None
    session_title: str | None = None
    session_id: str | None = None


class AttributeSessionOut(BaseModel):
    id: str = Field(alias="_id")
    selected_attributes: List[str]
    available_attributes: List[str]
    session_title: str | None = None
    is_active: bool | None = None
    updated_at: datetime | None = None


class ProductOut(BaseModel):
    id: str = Field(alias="_id")
    supplier_id: str
    supplier_sku: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    raw_attributes: Dict[str, Any]
    extracted_attributes: Dict[str, Any]
    mapped_attributes: Dict[str, Any]
    approval_status: str
    extraction_confidence: float
    created_at: datetime
    updated_at: datetime


class ProductCreate(BaseModel):
    supplier_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    supplier_sku: Optional[str] = None
    mapped_attributes: Optional[Dict[str, Any]] = None
    extracted_attributes: Optional[Dict[str, Any]] = None
    raw_attributes: Optional[Dict[str, Any]] = None


class ProductUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    mapped_attributes: Optional[Dict[str, Any]] = None


class ApprovalRequest(BaseModel):
    status: str


class ApprovalOut(BaseModel):
    id: str = Field(alias="_id")
    product_id: str
    status: str
    reviewer: Optional[str] = None
    notes: Optional[str] = None
    updated_at: datetime


class SyncEnqueueRequest(BaseModel):
    supplier_id: Optional[str] = None


class SyncProcessRequest(BaseModel):
    limit: int = 50


class SyncQueueOut(BaseModel):
    id: str = Field(alias="_id")
    product_id: str
    supplier_id: str
    status: str
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
