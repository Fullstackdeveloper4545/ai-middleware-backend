import csv
import io
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .openai_utils import DEFAULT_TARGET_ATTRIBUTES, extract_attributes_with_openai


def _decode_csv_bytes(raw: bytes) -> str:
    encodings = [
        "utf-8-sig",
        "utf-16",
        "utf-16-le",
        "utf-16-be",
        "cp1253",
        "iso-8859-7",
        "cp1252",
        "latin-1",
    ]
    for enc in encodings:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def parse_csv_bytes(raw: bytes) -> Tuple[List[str], List[Dict[str, Any]]]:
    text = _decode_csv_bytes(raw)
    sample = text[:4096]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample)
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    headers = [h.strip() for h in (reader.fieldnames or []) if h]
    rows: List[Dict[str, Any]] = []
    for row in reader:
        cleaned: Dict[str, Any] = {}
        for k, v in row.items():
            if not k:
                continue
            key = k.strip()
            val = v.strip() if isinstance(v, str) else v
            cleaned[key] = val
        if any(str(v).strip() for v in cleaned.values() if v is not None):
            rows.append(cleaned)
    return headers, rows


def score_confidence(extracted: Dict[str, Any]) -> float:
    filled = sum(1 for v in extracted.values() if isinstance(v, str) and v.strip())
    if filled >= 5:
        return 0.85
    if filled >= 3:
        return 0.65
    if filled >= 1:
        return 0.4
    return 0.2


def _strip_html(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"<[^>]+>", " ", value)


def _row_value(row: Dict[str, Any], *keys: str) -> str:
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        value = lowered.get(key.strip().lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _row_description_text(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for k, v in row.items():
        if v is None:
            continue
        key = str(k).strip().lower()
        if (
            "description" in key
            or key in {"desc", "details", "detail", "body", "body html", "body_html", "product_description", "summary"}
            or "\u03c0\u03b5\u03c1\u03b9\u03b3\u03c1\u03b1\u03c6\u03ae" in key
            or "\u03c0\u03b5\u03c1\u03b9\u03b3\u03c1\u03b1\u03c6\u03b7" in key
        ):
            text_val = _strip_html(str(v)).strip()
            if text_val:
                parts.append(text_val)
    if parts:
        return " ".join(parts)
    return _strip_html(_row_value(row, "description", "html_description"))


def _row_text_blob(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for v in row.values():
        if v is None:
            continue
        try:
            cleaned = _strip_html(str(v)).strip()
        except Exception:
            continue
        if cleaned:
            parts.append(cleaned)
    return " ".join(parts)


def _normalize_text(value: str) -> str:
    lowered = value.lower()
    return "".join(ch for ch in unicodedata.normalize("NFD", lowered) if unicodedata.category(ch) != "Mn")


def _normalize_attr_key(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("_", " ").replace("-", " ")
    return cleaned.lower()


def _prepare_targets(target_attributes: List[str] | None) -> List[tuple[str, str]]:
    if not target_attributes:
        defaults = DEFAULT_TARGET_ATTRIBUTES.copy()
        return [(item, _normalize_attr_key(item)) for item in defaults]
    prepared: List[tuple[str, str]] = []
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

COLOR_MODIFIERS = [
    "ÏƒÎºÎ¿ÏÏÎ¿",
    "ÏƒÎºÎ¿ÏÏÎ±",
    "Î±Î½Î¿Î¹Ï‡Ï„ÏŒ",
    "Î±Î½Î¿Î¹Ï‡Ï„Î®",
    "Î±Î½Î¿Î¹ÎºÏ„ÏŒ",
    "Î±Î½Î¿Î¹ÎºÏ„Î®",
    "dark",
    "light",
    "deep",
    "pale",
    "bright",
]

EN_COLOR_WORDS = [
    "black",
    "white",
    "grey",
    "gray",
    "blue",
    "red",
    "green",
    "brown",
    "beige",
    "pink",
    "yellow",
    "orange",
    "gold",
    "golden",
    "silver",
    "purple",
    "violet",
    "lavender",
    "magenta",
    "cyan",
    "teal",
    "turquoise",
    "navy",
    "olive",
    "lime",
    "maroon",
    "burgundy",
    "cream",
    "ivory",
    "off-white",
    "charcoal",
    "graphite",
    "chocolate",
    "coffee",
    "sand",
    "khaki",
    "peach",
    "coral",
    "salmon",
    "fuchsia",
    "lilac",
    "mint",
    "emerald",
    "sapphire",
    "ruby",
    "bronze",
    "copper",
]

DEFAULT_GREEK_COLOR_WORDS: List[str] = []


def _load_greek_colors() -> List[str]:
    path = Path(__file__).resolve().parent / "data" / "greek_colors.txt"
    if not path.exists():
        return DEFAULT_GREEK_COLOR_WORDS
    colors: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        colors.append(item.lower())
    deduped = list(dict.fromkeys(colors))
    return deduped or DEFAULT_GREEK_COLOR_WORDS


def _get_color_keywords() -> list[tuple[str, str]]:
    greek_colors = _load_greek_colors()
    keywords: list[tuple[str, str]] = []
    seen: set[str] = set()
    for mod in COLOR_MODIFIERS:
        for color in EN_COLOR_WORDS + greek_colors:
            for canonical in (f"{mod} {color}", f"{mod}-{color}"):
                norm = _normalize_text(canonical)
                if norm and norm not in seen:
                    keywords.append((canonical, norm))
                    seen.add(norm)
    for color in EN_COLOR_WORDS + greek_colors:
        norm = _normalize_text(color)
        if norm and norm not in seen:
            keywords.append((color, norm))
            seen.add(norm)
    return keywords


def _find_keyword_in_text(text: str, keywords: List[str]) -> str:
    if not text:
        return ""
    normalized_text = _normalize_text(text)
    best_index = None
    best_match = ""
    best_len = 0
    for keyword in keywords:
        if not keyword:
            continue
        normalized_kw = _normalize_text(keyword)
        pattern = re.compile(r"(?<!\w)" + re.escape(normalized_kw) + r"(?!\w)", flags=re.IGNORECASE)
        for match in pattern.finditer(normalized_text):
            idx = match.start()
            length = len(match.group(0))
            if best_index is None or idx < best_index or (idx == best_index and length > best_len):
                best_index = idx
                best_len = length
                best_match = keyword
    return best_match


def _infer_color_from_text(text: str) -> str:
    if not text:
        return ""
    labeled = _extract_labeled_color(text)
    if labeled:
        return labeled
    soft = _extract_soft_color(text)
    if soft:
        return soft
    return _extract_color_keyword(text)


def _extract_labeled_color(text: str) -> str:
    patterns = [
        r"\b(?:color|colour|Ï‡ÏÏŽÎ¼Î±|Ï‡ÏÏ‰Î¼Î±)\s*[:\-â€“]\s*([^\n]+)",
        r"\b(?:color|colour)\s+is\s+([^\n]+)",
        r"\b(?:Ï‡ÏÏŽÎ¼Î±|Ï‡ÏÏ‰Î¼Î±)\s+ÎµÎ¯Î½Î±Î¹\s+([^\n]+)",
    ]
    split_markers = r"\b(?:material|fabric|Ï…Î»Î¹ÎºÏŒ|Ï…Î»Î¹Îº[ÏŒÎ¿]|dimensions|Î´Î¹Î±ÏƒÏ„Î¬ÏƒÎµÎ¹Ï‚|ÏƒÏ…Î½Î¿Î»Î¹ÎºÎ­Ï‚)\b"
    fallback = ""
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            value = match.group(1).strip()
            if not value:
                continue
            value = re.split(split_markers, value, maxsplit=1, flags=re.IGNORECASE)[0]
            value = value.strip(" \t\r\n;,:-â€“")
            if not value:
                continue
            keyword = _extract_color_keyword(value)
            if keyword:
                return keyword
            fallback = value
    return fallback


def _extract_color_keyword(text: str) -> str:
    if not text:
        return ""
    best_index = None
    best_match = ""
    best_len = 0
    normalized_text = _normalize_text(text)
    for canonical, normalized_kw in _get_color_keywords():
        if not normalized_kw:
            continue
        pattern = re.compile(r"(?<!\w)" + re.escape(normalized_kw) + r"(?!\w)", flags=re.IGNORECASE)
        for match in pattern.finditer(normalized_text):
            idx = match.start()
            length = len(match.group(0))
            if best_index is None or idx < best_index or (idx == best_index and length > best_len):
                best_index = idx
                best_len = length
                best_match = canonical
    return best_match


def _extract_soft_color(text: str) -> str:
    greek_soft = [
        "Î¼Î±Î»Î±ÎºÏŒ",
        "Î¼Î±Î»Î±ÎºÎ®",
        "Î¼Î±Î»Î±ÎºÏŒÏ‚",
    ]
    patterns = [
        rf"\b(?:{greek_soft[0]}|{greek_soft[1]}|{greek_soft[2]})\s+([^\s:;,.]+)",
        r"\bsoft\s+([^\s:;,.]+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = match.group(1).strip()
            if not candidate:
                continue
            keyword = _extract_color_keyword(candidate)
            if keyword:
                return keyword
    return ""


def _infer_material_from_text(text: str) -> str:
    if not text:
        return ""
    keywords = [
        "velvet",
        "velour",
        "fabric",
        "???????",
        "??????",
        "??????????",
        "???????????",
        "??????????",
        "??????????",
    ]
    return _find_keyword_in_text(text, keywords)


def ai_extract(row: Dict[str, Any], target_attributes: List[str] | None = None) -> Dict[str, Any]:
    target_pairs = _prepare_targets(target_attributes)
    normalized_row = {
        _normalize_attr_key(str(k)): (str(v).strip() if isinstance(v, str) else v)
        for k, v in row.items()
        if k
    }
    base: Dict[str, Any] = {}
    for original_key, normalized_key in target_pairs:
        if normalized_key == "title":
            base[original_key] = _row_value(row, "title", "product_title", "product_name", "name")
        elif normalized_key == "description":
            base[original_key] = _row_description_text(row)
        elif normalized_key == "sku":
            base[original_key] = _row_value(row, "sku", "supplier_sku", "supplier sku", "item_sku", "item sku", "variant_sku", "variant sku")
        else:
            base[original_key] = normalized_row.get(normalized_key, "") or ""

    needs_ai = any(not (isinstance(v, str) and v.strip()) for v in base.values())
    extracted = extract_attributes_with_openai(row, [original for original, _ in target_pairs]) or {} if needs_ai else {}

    merged: Dict[str, Any] = {}
    for original_key, _ in target_pairs:
        merged[original_key] = base.get(original_key) or extracted.get(original_key) or ""

    target_key_by_normalized = {normalized_key: original_key for original_key, normalized_key in target_pairs}
    context_title = _row_value(row, "title", "product_title", "product_name", "name")
    context_desc = _row_description_text(row)
    context_blob = _row_text_blob(row)
    context = " ".join([context_title, context_desc, context_blob]).strip()

    description_key = target_key_by_normalized.get("description")
    if description_key:
        # Keep original CSV description stable; never let AI rewrite it.
        merged[description_key] = base.get(description_key) or ""

    color_key = target_key_by_normalized.get("color")
    if color_key:
        csv_color = base.get(color_key) or ""
        if isinstance(csv_color, str) and csv_color.strip():
            merged[color_key] = csv_color
        else:
            desc_color = _infer_color_from_text(context_desc)
            if not desc_color:
                desc_color = _infer_color_from_text(context_blob)
            if not desc_color:
                desc_color = _infer_color_from_text(context)
            merged[color_key] = desc_color or ""

    material_key = target_key_by_normalized.get("material")
    if material_key and not merged[material_key]:
        merged[material_key] = _infer_material_from_text(context)

    fabric_key = target_key_by_normalized.get("fabric")
    if fabric_key and not merged[fabric_key]:
        merged[fabric_key] = _infer_material_from_text(context)

    return merged


def extract_supplier_sku(row: Dict[str, Any]) -> str:
    return _row_value(row, "supplier_sku", "sku")


def extract_title(row: Dict[str, Any]) -> str:
    return _row_value(row, "title", "product_title", "product_name", "name")


def extract_description(row: Dict[str, Any]) -> str:
    return _row_description_text(row)


def map_to_master_attributes(extracted: Dict[str, Any], active_attributes: List[str] | None = None) -> Dict[str, Any]:
    if active_attributes:
        lower = {_normalize_attr_key(str(k)): v for k, v in extracted.items()}
        mapped: Dict[str, Any] = {}
        for attr in active_attributes:
            name = str(attr or "").strip()
            if not name:
                continue
            mapped[name] = lower.get(_normalize_attr_key(name), "")
        return mapped
    return {
        "Color": extracted.get("color") or "",
        "Material": extracted.get("material") or "",
        "Size": extracted.get("size") or "",
        "Style": extracted.get("style") or "",
        "Drawers": extracted.get("drawers") or "",
    }


def now_utc() -> datetime:
    return datetime.utcnow()
