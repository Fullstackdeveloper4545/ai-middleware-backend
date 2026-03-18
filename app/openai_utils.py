from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, Optional

from openai import OpenAI


DEFAULT_TARGET_ATTRIBUTES = [
    "title",
    "description",
    "color",
    "material",
    "size",
    "style",
    "drawers",
]


def get_client() -> Optional[OpenAI]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_api_key":
        return None
    return OpenAI(api_key=api_key, timeout=8.0, max_retries=0)


def _normalize_attr_key(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("_", " ").replace("-", " ")
    return cleaned.lower()


def _prepare_target_map(target_attributes: Optional[Iterable[str]]) -> list[tuple[str, str]]:
    if not target_attributes:
        defaults = DEFAULT_TARGET_ATTRIBUTES.copy()
        return [(item, _normalize_attr_key(item)) for item in defaults]
    prepared: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in target_attributes:
        original = str(item or "").strip()
        normalized = _normalize_attr_key(original)
        if not original or not normalized or normalized in seen:
            continue
        prepared.append((original, normalized))
        seen.add(normalized)
    if prepared:
        return prepared
    defaults = DEFAULT_TARGET_ATTRIBUTES.copy()
    return [(item, _normalize_attr_key(item)) for item in defaults]

def extract_attributes_with_openai(
    row: Dict[str, Any],
    target_attributes: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    client = get_client()
    if client is None:
        return {}

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    target_map = _prepare_target_map(target_attributes)
    target_list = ", ".join(normalized for _, normalized in target_map)
    system = (
        "You extract product attributes from supplier CSV data.\n"
        f"Master attributes (exact keys, lowercase): {target_list}.\n"
        "Return ONLY valid JSON with these exact keys.\n"
        "Rules:\n"
        "1) Read the CSV row and map any columns that directly match the master attributes.\n"
        "2) For any master attribute that is missing/empty, read the product title; if still missing, read the description line by line and look for clues about that attribute.\n"
        "3) Extract relevant info from title/description using the wording as-is (do not translate). Recognize color words in any language and return them in the source language.\n"
        "4) If found, fill the attribute; if not, set it to \"\" (empty string).\n"
        "5) Always return all master attribute keys, and output ONLY the JSON object - no extra text."
    )

    user = "Supplier row data (JSON):\n" + json.dumps(row, ensure_ascii=False)

    try:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
        )

        text = resp.output_text
        if not text:
            return {}

        data = json.loads(text)
        if isinstance(data, dict):
            normalized = {_normalize_attr_key(k): v for k, v in data.items()}
            return {
                original: str(normalized.get(normalized_key) or "")
                for original, normalized_key in target_map
            }
    except Exception:
        return {}

    return {}
