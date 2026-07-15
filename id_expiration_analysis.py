import base64
import binascii
import csv
from dataclasses import dataclass, field
from datetime import date, datetime
import json
from pathlib import Path
import re
from time import perf_counter
import unicodedata

from dotenv import load_dotenv
from sqlalchemy import Table

load_dotenv(Path(__file__).resolve().with_name(".env"), override=False)

from src.components.clients.document_processing import (
    GOOGLE_DOCUMENT_AI_PROVIDER,
    OCR_MAX_RENDER_DIMENSION,
    OCR_RENDER_DPI,
    assess_ocr_text,
    extract_document_ocr,
    validate_ocr_provider_configuration,
)
from src.utils.connectors.supabase import db

CATEGORY = "Proof of Identity"
TARGET_COUNT = 25
PREVIEW_LENGTH = 300
DETAIL_CSV_PATH = "contact_document_ocr_extractions.csv"
SUMMARY_CSV_PATH = "contact_document_ocr_summary.csv"
MRZ_WEIGHTS = (7, 3, 1)
MRZ_DIGIT_REPLACEMENTS = str.maketrans(
    {
        "O": "0",
        "Q": "0",
        "I": "1",
        "L": "1",
        "Z": "2",
        "S": "5",
        "G": "6",
        "B": "8",
    }
)

MONTH_NUMBERS = {
    "JAN": 1,
    "ENE": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "ABR": 4,
    "AVR": 4,
    "MAY": 5,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "AGO": 8,
    "SEP": 9,
    "SET": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
    "DIC": 12,
}

NUMERIC_DATE_PATTERN = re.compile(
    r"(?<!\d)(\d{1,2})\s*[/\.\-\s]\s*(\d{1,2})"
    r"\s*[/\.\-\s]\s*(\d{2,4})(?!\d)"
)
MERGED_DAY_MONTH_DATE_PATTERN = re.compile(
    r"(?<!\d)(\d{2})(\d{2})(?:\s*[/\.\-]\s*|\s+)(\d{4})(?!\d)"
)
COMPACT_DATE_PATTERN = re.compile(r"(?<!\d)(\d{8})(?!\d)")
TEXT_MONTH_DATE_PATTERN = re.compile(
    r"(?<!\d)(\d{1,2})\s*[/\.\-\s]+\s*"
    r"([A-Za-zÀ-ÿ]{3,16})(?:\s*[/\.\-]\s*[A-Za-zÀ-ÿ]{3,16})?"
    r"\s*[/\.\-\s]+\s*(\d{2,4})(?!\d)",
    flags=re.IGNORECASE,
)

LABEL_PATTERNS = {
    "expiry": re.compile(
        r"(?:"
        r"\bfecha\s+(?:de\s+)?(?:venc\w*|caduc\w*|expir\w*)\b|"
        r"\bdate\s+(?:of\s+|d\s*)?(?:expiry|expiration|expir\w*)\b|"
        r"\b(?:venc\w*|vence|caduc\w*|expir\w*)\b|"
        r"\bvalid\w*\s+(?:until|through|thru|hasta)\b"
        r")",
        flags=re.IGNORECASE,
    ),
    "birth": re.compile(
        r"(?:"
        r"\bfecha\s+(?:de\s+)?nac\w*\b|"
        r"\bf\.?\s*nac\.?\b|"
        r"\bdate\s+(?:of\s+|de\s+)?birth\b|"
        r"\bbirth\s+date\b|"
        r"\bdate\s+de\s+naissance\b|"
        r"\bdata\s+(?:de\s+)?nascimento\b"
        r")",
        flags=re.IGNORECASE,
    ),
    "issue": re.compile(
        r"(?:"
        r"\bfecha\s+(?:y\s+lugar\s+)?(?:de\s+)?exped\w*\b|"
        r"\bdate\s+(?:of\s+|de\s+)?issue\b|"
        r"\bissue\s+date\b|"
        r"\bdate\s+de\s+delivrance\b|"
        r"\bfecha\s+(?:de\s+)?emisi\w*\b"
        r")",
        flags=re.IGNORECASE,
    ),
}

UNICODE_LOOKALIKE_TRANSLATION = str.maketrans(
    {
        # Greek and Cyrillic characters that OCR commonly substitutes for Latin.
        "Α": "A", "Β": "B", "Ε": "E", "Ζ": "Z", "Η": "H",
        "Ι": "I", "Κ": "K", "Μ": "M", "Ν": "N", "Ο": "O",
        "Ρ": "P", "Τ": "T", "Υ": "Y", "Χ": "X",
        "а": "a", "е": "e", "о": "o", "р": "p", "с": "c",
        "х": "x", "у": "y", "А": "A", "В": "B", "Е": "E",
        "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P",
        "С": "C", "Т": "T", "У": "Y", "Х": "X",
    }
)


@dataclass(slots=True)
class DateEvidence:
    value: date
    raw: str
    source: str
    label: str | None
    score: float
    evidence: str
    mrz_format: str | None = None
    mrz_expiry_check_valid: bool | None = None
    mrz_birth_check_valid: bool | None = None

    def as_dict(self) -> dict:
        return {
            "date": self.value.isoformat(),
            "raw": self.raw,
            "source": self.source,
            "label": self.label,
            "score": round(self.score, 1),
            "evidence": self.evidence,
            "mrz_format": self.mrz_format,
            "mrz_expiry_check_valid": self.mrz_expiry_check_valid,
            "mrz_birth_check_valid": self.mrz_birth_check_valid,
        }


@dataclass(slots=True)
class RankedExpiryCandidate:
    value: date
    score: float
    sources: list[str]
    labels: list[str]
    evidence: list[str]
    mrz_formats: list[str]
    mrz_expiry_check_valid: bool | None

    def as_dict(self) -> dict:
        return {
            "date": self.value.isoformat(),
            "score": round(self.score, 1),
            "sources": self.sources,
            "labels": self.labels,
            "evidence": self.evidence,
            "mrz_formats": self.mrz_formats,
            "mrz_expiry_check_valid": self.mrz_expiry_check_valid,
        }


@dataclass(slots=True)
class IDExpiryAnalysis:
    status: str
    proposed_expiry_date: str | None
    confidence: str
    candidate_conflict: bool
    candidates: list[RankedExpiryCandidate] = field(default_factory=list)
    all_date_candidates: list[str] = field(default_factory=list)
    labeled_expiry_dates: list[str] = field(default_factory=list)
    mrz_expiry_dates: list[str] = field(default_factory=list)
    mrz_formats: list[str] = field(default_factory=list)
    mrz_expiry_check_valid: bool | None = None
    existing_expiry_date: str | None = None
    matches_existing_expiry: bool | None = None
    review_reasons: list[str] = field(default_factory=list)


def preview_text(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "<empty>"
    return normalized[:PREVIEW_LENGTH]


def _normalize_search_text(value: str) -> str:
    lookalikes_normalized = str(value or "").translate(
        UNICODE_LOOKALIKE_TRANSLATION
    )
    decomposed = unicodedata.normalize("NFKD", lookalikes_normalized)
    return "".join(
        character
        for character in decomposed
        if not unicodedata.combining(character)
    )


def _compact_evidence(value: str, limit: int = 180) -> str:
    compact = re.sub(r"\s+", " ", str(value or "")).strip()
    return compact[:limit]


def _resolve_visual_year(raw_year: str, today: date) -> int:
    year = int(raw_year)
    if len(raw_year) == 4:
        return year
    future_cutoff = (today.year + 30) % 100
    return 2000 + year if year <= future_cutoff else 1900 + year


def _parse_date_parts(
    day: str,
    month: int,
    raw_year: str,
    today: date,
) -> date | None:
    try:
        year = _resolve_visual_year(raw_year, today)
        if year < 1900 or year > today.year + 40:
            return None
        return date(year, month, int(day))
    except (TypeError, ValueError):
        return None


def _month_number(raw_month: str) -> int | None:
    normalized = _normalize_search_text(raw_month).upper()
    for token, month_number in MONTH_NUMBERS.items():
        if token in normalized:
            return month_number
    return None


def _nearest_date_label(normalized_text: str, start: int, end: int) -> str | None:
    before_start = max(0, start - 180)
    before = normalized_text[before_start:start]
    nearest: tuple[int, str] | None = None
    for label, pattern in LABEL_PATTERNS.items():
        for match in pattern.finditer(before):
            distance = len(before) - match.end()
            if nearest is None or distance < nearest[0]:
                nearest = (distance, label)
    if nearest and nearest[0] <= 120:
        return nearest[1]

    after = normalized_text[end : min(len(normalized_text), end + 70)]
    following: tuple[int, str] | None = None
    for label, pattern in LABEL_PATTERNS.items():
        match = pattern.search(after)
        if match and (following is None or match.start() < following[0]):
            following = (match.start(), label)
    return following[1] if following and following[0] <= 35 else None


def _line_spans(value: str) -> list[tuple[int, int]]:
    spans = []
    offset = 0
    for line in value.splitlines(keepends=True):
        spans.append((offset, offset + len(line)))
        offset += len(line)
    if not spans or offset < len(value):
        spans.append((offset, len(value)))
    return spans


def _date_label_assignments(
    normalized_text: str,
    parsed_matches: list[tuple[date, str, int, int, bool]],
) -> dict[int, str]:
    """Pair labels and dates in reading order without reusing one nearby label."""

    spans = _line_spans(normalized_text)
    dates_by_line: dict[int, list[tuple[date, str, int, int, bool]]] = {}
    labels_by_line: dict[int, list[tuple[int, int, str]]] = {}
    for item in parsed_matches:
        line_number = next(
            (
                index
                for index, (start, end) in enumerate(spans)
                if start <= item[2] < end
            ),
            len(spans) - 1,
        )
        dates_by_line.setdefault(line_number, []).append(item)

    for line_number, (line_start, line_end) in enumerate(spans):
        line_labels = []
        line_text = normalized_text[line_start:line_end]
        for label, pattern in LABEL_PATTERNS.items():
            for match in pattern.finditer(line_text):
                line_labels.append(
                    (line_start + match.start(), line_start + match.end(), label)
                )
        line_labels.sort()
        compressed = []
        for label_event in line_labels:
            if (
                compressed
                and compressed[-1][2] == label_event[2]
                and label_event[0] - compressed[-1][1] <= 80
            ):
                compressed[-1] = (
                    compressed[-1][0],
                    label_event[1],
                    label_event[2],
                )
            else:
                compressed.append(label_event)
        if compressed:
            labels_by_line[line_number] = compressed

    assignments: dict[int, str] = {}
    pending_labels: list[tuple[int, int, int, str]] = []
    for line_number in range(len(spans)):
        line_labels = labels_by_line.get(line_number, [])
        line_dates = sorted(dates_by_line.get(line_number, []), key=lambda item: item[2])
        if line_labels and line_dates:
            available_labels = list(line_labels)
            if len(available_labels) == len(line_dates):
                for date_item, label_event in zip(line_dates, available_labels):
                    assignments[date_item[2]] = label_event[2]
            else:
                for date_item in line_dates:
                    nearest = min(
                        available_labels,
                        key=lambda event: min(
                            abs(date_item[2] - event[1]),
                            abs(event[0] - date_item[3]),
                        ),
                    )
                    assignments[date_item[2]] = nearest[2]
                    if len(available_labels) > 1:
                        available_labels.remove(nearest)
            pending_labels.clear()
            continue

        if line_labels:
            pending_labels.extend(
                (line_number, start, end, label)
                for start, end, label in line_labels
            )
            continue

        if not line_dates:
            continue
        pending_labels = [
            event
            for event in pending_labels
            if line_number - event[0] <= 4
            and line_dates[0][2] - event[2] <= 400
        ]
        for date_item in line_dates:
            if pending_labels:
                newest_label_line = max(event[0] for event in pending_labels)
                if any(
                    event[0] != newest_label_line
                    for event in pending_labels
                ):
                    pending_labels = [
                        event
                        for event in pending_labels
                        if event[0] == newest_label_line
                    ]
                assignments[date_item[2]] = pending_labels.pop(0)[3]
            else:
                fallback = _nearest_date_label(
                    normalized_text,
                    date_item[2],
                    date_item[3],
                )
                if fallback:
                    assignments[date_item[2]] = fallback
    return assignments


def _parse_compact_date(raw_value: str, today: date) -> date | None:
    if len(raw_value) != 8 or not raw_value.isdigit():
        return None
    if 1900 <= int(raw_value[:4]) <= today.year + 40:
        try:
            return date(
                int(raw_value[:4]),
                int(raw_value[4:6]),
                int(raw_value[6:8]),
            )
        except ValueError:
            pass
    return _parse_date_parts(raw_value[:2], int(raw_value[2:4]), raw_value[4:], today)


def _visual_date_score(
    value: date,
    label: str | None,
    latest_visual_date: date,
    today: date,
) -> float:
    score = 20.0
    if label == "expiry":
        score += 55.0
    elif label == "birth":
        score -= 45.0
    elif label == "issue":
        score -= 25.0

    if today.year - 20 <= value.year <= today.year + 30:
        score += 10.0
    if value >= today:
        score += 15.0
    if value == latest_visual_date and label not in {"birth", "issue"}:
        score += 10.0
    return max(0.0, score)


def _extract_visual_date_evidence(text: str, today: date) -> list[DateEvidence]:
    normalized_text = _normalize_search_text(text)
    parsed_matches: list[tuple[date, str, int, int, bool]] = []
    occupied_ranges: list[tuple[int, int]] = []

    for match in TEXT_MONTH_DATE_PATTERN.finditer(normalized_text):
        month = _month_number(match.group(2))
        if month is None:
            continue
        parsed_date = _parse_date_parts(match.group(1), month, match.group(3), today)
        if parsed_date is None:
            continue
        parsed_matches.append(
            (parsed_date, match.group(0), match.start(), match.end(), False)
        )
        occupied_ranges.append((match.start(), match.end()))

    for match in MERGED_DAY_MONTH_DATE_PATTERN.finditer(normalized_text):
        line_start = normalized_text.rfind("\n", 0, match.start()) + 1
        line_end = normalized_text.find("\n", match.end())
        if line_end < 0:
            line_end = len(normalized_text)
        local_label = _nearest_date_label(
            normalized_text[line_start:line_end],
            match.start() - line_start,
            match.end() - line_start,
        )
        if local_label is None:
            continue
        parsed_date = _parse_date_parts(
            match.group(1),
            int(match.group(2)),
            match.group(3),
            today,
        )
        if parsed_date is None:
            continue
        parsed_matches.append(
            (parsed_date, match.group(0), match.start(), match.end(), True)
        )
        occupied_ranges.append((match.start(), match.end()))

    for match in NUMERIC_DATE_PATTERN.finditer(normalized_text):
        if any(
            range_start < match.end() and match.start() < range_end
            for range_start, range_end in occupied_ranges
        ):
            continue
        parsed_date = _parse_date_parts(
            match.group(1),
            int(match.group(2)),
            match.group(3),
            today,
        )
        if parsed_date is None:
            continue
        parsed_matches.append(
            (parsed_date, match.group(0), match.start(), match.end(), False)
        )

    for match in COMPACT_DATE_PATTERN.finditer(normalized_text):
        if any(
            range_start < match.end() and match.start() < range_end
            for range_start, range_end in occupied_ranges
        ):
            continue
        line_start = normalized_text.rfind("\n", 0, match.start()) + 1
        line_end = normalized_text.find("\n", match.end())
        if line_end < 0:
            line_end = len(normalized_text)
        local_label = _nearest_date_label(
            normalized_text[line_start:line_end],
            match.start() - line_start,
            match.end() - line_start,
        )
        if local_label is None:
            continue
        parsed_date = _parse_compact_date(match.group(1), today)
        if parsed_date is None:
            continue
        parsed_matches.append(
            (parsed_date, match.group(0), match.start(), match.end(), True)
        )

    if not parsed_matches:
        return []

    latest_visual_date = max(item[0] for item in parsed_matches)
    label_assignments = _date_label_assignments(normalized_text, parsed_matches)
    evidence = []
    for parsed_date, raw, start, end, compact in sorted(
        parsed_matches,
        key=lambda item: item[2],
    ):
        label = label_assignments.get(start) or _nearest_date_label(
            normalized_text,
            start,
            end,
        )
        context_start = max(0, start - 90)
        context_end = min(len(normalized_text), end + 90)
        evidence.append(
            DateEvidence(
                value=parsed_date,
                raw=raw,
                source=(
                    "text_labeled_compact"
                    if compact and label
                    else "text_labeled"
                    if label
                    else "text_unlabeled"
                ),
                label=label,
                score=_visual_date_score(
                    parsed_date,
                    label,
                    latest_visual_date,
                    today,
                ),
                evidence=_compact_evidence(
                    normalized_text[context_start:context_end]
                ),
            )
        )
    return evidence


def _normalize_mrz_line(raw_line: str) -> str:
    normalized = _normalize_search_text(raw_line).upper().replace("«", "<")
    return re.sub(r"[^A-Z0-9<]", "", normalized)


def _normalize_mrz_digits(value: str) -> str | None:
    normalized = str(value or "").upper().translate(MRZ_DIGIT_REPLACEMENTS)
    return normalized if normalized.isdigit() else None


def _mrz_character_value(character: str) -> int:
    if character.isdigit():
        return int(character)
    if "A" <= character <= "Z":
        return ord(character) - ord("A") + 10
    if character == "<":
        return 0
    raise ValueError(f"Unsupported MRZ character: {character}")


def _mrz_check_valid(value: str, raw_check_digit: str) -> bool:
    check_digit = _normalize_mrz_digits(raw_check_digit)
    if check_digit is None or len(check_digit) != 1:
        return False
    try:
        calculated = sum(
            _mrz_character_value(character) * MRZ_WEIGHTS[index % 3]
            for index, character in enumerate(value)
        ) % 10
    except ValueError:
        return False
    return calculated == int(check_digit)


def _parse_mrz_date(raw_value: str, *, expiry: bool, today: date) -> date | None:
    digits = _normalize_mrz_digits(raw_value)
    if digits is None or len(digits) != 6:
        return None
    year_suffix = int(digits[:2])
    if expiry:
        future_cutoff = (today.year + 30) % 100
        year = 2000 + year_suffix if year_suffix <= future_cutoff else 1900 + year_suffix
    else:
        year = 2000 + year_suffix
        if year > today.year:
            year -= 100
        if today.year - year > 120:
            return None
    try:
        return date(year, int(digits[2:4]), int(digits[4:6]))
    except ValueError:
        return None


def _mrz_evidence(
    line: str,
    *,
    mrz_format: str,
    birth_value: str,
    birth_check_digit: str,
    expiry_value: str,
    expiry_check_digit: str,
    today: date,
) -> DateEvidence | None:
    normalized_birth = _normalize_mrz_digits(birth_value)
    normalized_expiry = _normalize_mrz_digits(expiry_value)
    if normalized_birth is None or normalized_expiry is None:
        return None
    birth_date = _parse_mrz_date(normalized_birth, expiry=False, today=today)
    expiry_date = _parse_mrz_date(normalized_expiry, expiry=True, today=today)
    if birth_date is None or expiry_date is None:
        return None
    birth_check_valid = _mrz_check_valid(normalized_birth, birth_check_digit)
    expiry_check_valid = _mrz_check_valid(normalized_expiry, expiry_check_digit)
    score = 80.0 if expiry_check_valid else 45.0
    if birth_check_valid:
        score += 5.0
    if expiry_date >= today:
        score += 5.0
    return DateEvidence(
        value=expiry_date,
        raw=expiry_value,
        source="mrz",
        label="expiry",
        score=score,
        evidence=line,
        mrz_format=mrz_format,
        mrz_expiry_check_valid=expiry_check_valid,
        mrz_birth_check_valid=birth_check_valid,
    )


def _extract_mrz_date_evidence(
    text: str,
    today: date,
    ocr_lines: list[str] | None = None,
) -> list[DateEvidence]:
    evidence: list[DateEvidence] = []
    seen = set()
    raw_lines = str(text or "").splitlines()
    raw_lines.extend(ocr_lines or [])
    lines = list(
        dict.fromkeys(
            line
            for line in (_normalize_mrz_line(raw_line) for raw_line in raw_lines)
            if line
        )
    )

    def add_parsed(parsed: DateEvidence | None) -> None:
        if parsed is None:
            return
        key = (
            parsed.value,
            parsed.mrz_format,
            parsed.mrz_expiry_check_valid,
        )
        if key not in seen:
            seen.add(key)
            evidence.append(parsed)

    for line_index, line in enumerate(lines):
        previous_is_td3_header = bool(
            line_index > 0 and lines[line_index - 1].startswith("P<")
        )
        if previous_is_td3_header or 44 <= len(line) <= 48:
            for offset in range(0, max(1, len(line) - 43)):
                window = line[offset : offset + 44]
                if len(window) != 44 or window[20] not in {"M", "F", "X", "<"}:
                    continue
                add_parsed(
                    _mrz_evidence(
                        window,
                        mrz_format="TD3",
                        birth_value=window[13:19],
                        birth_check_digit=window[19],
                        expiry_value=window[21:27],
                        expiry_check_digit=window[27],
                        today=today,
                    )
                )

        td1_candidates = []
        if 18 <= len(line) <= 34:
            td1_candidates.extend(
                line[offset : offset + 30]
                for offset in range(0, len(line) - 17)
            )
        td1_start = line.find("ID")
        if td1_start >= 0 and len(line) - td1_start >= 60:
            merged_td1 = line[td1_start:]
            td1_candidates.extend(
                merged_td1[offset : offset + 30]
                for offset in range(30, len(merged_td1) - 29, 30)
            )
        for window in td1_candidates:
            if window[7] not in {"M", "F", "X", "<"}:
                continue
            parsed = _mrz_evidence(
                window,
                mrz_format="TD1",
                birth_value=window[0:6],
                birth_check_digit=window[6],
                expiry_value=window[8:14],
                expiry_check_digit=window[14],
                today=today,
            )
            if parsed is None:
                continue
            if len(window) < 30 and (
                len(window) < 18
                or not re.fullmatch(r"[A-Z<]{3}", window[15:18])
                or parsed.mrz_birth_check_valid is not True
                or parsed.mrz_expiry_check_valid is not True
            ):
                continue
            add_parsed(parsed)
    return evidence


def _parse_existing_expiry(value: object) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    iso_candidate = normalized[:10]
    try:
        return date.fromisoformat(iso_candidate).isoformat()
    except ValueError:
        pass
    for date_format in ("%d/%m/%Y", "%d-%m-%Y", "%d %m %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(normalized, date_format).date().isoformat()
        except ValueError:
            continue
    return None


def _rank_expiry_candidates(
    evidence: list[DateEvidence],
    existing_expiry_date: str | None,
) -> list[RankedExpiryCandidate]:
    grouped: dict[str, list[DateEvidence]] = {}
    for item in evidence:
        grouped.setdefault(item.value.isoformat(), []).append(item)

    ranked = []
    for iso_date, items in grouped.items():
        sources = sorted({item.source for item in items})
        labels = sorted({item.label for item in items if item.label})
        mrz_formats = sorted({item.mrz_format for item in items if item.mrz_format})
        valid_mrz = any(item.mrz_expiry_check_valid is True for item in items)
        invalid_mrz = any(item.mrz_expiry_check_valid is False for item in items)
        has_labeled_expiry = any(
            item.source.startswith("text_labeled") and item.label == "expiry"
            for item in items
        )
        score = max(item.score for item in items)
        if valid_mrz and has_labeled_expiry:
            score += 30.0
        elif len(sources) > 1:
            score += 15.0
        if existing_expiry_date == iso_date:
            score += 15.0
        ranked.append(
            RankedExpiryCandidate(
                value=items[0].value,
                score=score,
                sources=sources,
                labels=labels,
                evidence=list(dict.fromkeys(item.evidence for item in items))[:4],
                mrz_formats=mrz_formats,
                mrz_expiry_check_valid=(
                    True if valid_mrz else False if invalid_mrz else None
                ),
            )
        )
    return sorted(ranked, key=lambda candidate: (candidate.score, candidate.value), reverse=True)


def analyze_id_expiry(
    text: str,
    existing_expiry: object = None,
    *,
    today: date | None = None,
    ocr_lines: list[str] | None = None,
) -> IDExpiryAnalysis:
    analysis_date = today or date.today()
    visual_evidence = _extract_visual_date_evidence(text, analysis_date)
    mrz_evidence = _extract_mrz_date_evidence(
        text,
        analysis_date,
        ocr_lines=ocr_lines,
    )
    all_evidence = visual_evidence + mrz_evidence
    parsed_existing_expiry = _parse_existing_expiry(existing_expiry)
    ranked = _rank_expiry_candidates(all_evidence, parsed_existing_expiry)

    viable = [candidate for candidate in ranked if candidate.score >= 40.0]
    proposed = viable[0] if viable else None
    labeled_expiry_dates = sorted(
        {
            item.value.isoformat()
            for item in visual_evidence
            if item.label == "expiry"
        }
    )
    valid_mrz_dates = {
        item.value.isoformat()
        for item in mrz_evidence
        if item.mrz_expiry_check_valid is True
    }
    labeled_dates = set(labeled_expiry_dates)
    strong_disagreement = bool(
        valid_mrz_dates
        and labeled_dates
        and not valid_mrz_dates.intersection(labeled_dates)
    )
    close_second_candidate = bool(
        len(viable) > 1
        and viable[1].score >= 65.0
        and viable[0].score - viable[1].score <= 15.0
    )
    candidate_conflict = strong_disagreement or close_second_candidate
    proposed_iso = proposed.value.isoformat() if proposed else None
    matches_existing = (
        proposed_iso == parsed_existing_expiry
        if proposed_iso and parsed_existing_expiry
        else None
    )

    if proposed is None:
        status = "not_found"
        confidence = "none"
    elif candidate_conflict:
        status = "conflict"
        confidence = "low"
    elif proposed.score >= 90.0 and (
        proposed.mrz_expiry_check_valid is True or "expiry" in proposed.labels
    ):
        status = "high"
        confidence = "high"
    elif proposed.score >= 55.0:
        status = "review"
        confidence = "medium"
    else:
        status = "review"
        confidence = "low"
    if matches_existing is False:
        status = "conflict"
    review_reasons = []
    if proposed is None:
        review_reasons.append("expiry_not_found")
    if candidate_conflict:
        review_reasons.append("expiry_candidate_conflict")
    if proposed and confidence != "high":
        review_reasons.append("expiry_candidate_needs_review")
    if any(item.mrz_expiry_check_valid is False for item in mrz_evidence):
        review_reasons.append("mrz_expiry_check_failed")
    if str(existing_expiry or "").strip() and parsed_existing_expiry is None:
        review_reasons.append("existing_expiry_unparseable")
    if matches_existing is False:
        review_reasons.append("existing_expiry_conflict")

    return IDExpiryAnalysis(
        status=status,
        proposed_expiry_date=proposed_iso,
        confidence=confidence,
        candidate_conflict=candidate_conflict,
        candidates=ranked,
        all_date_candidates=sorted(
            {item.value.isoformat() for item in all_evidence}
        ),
        labeled_expiry_dates=labeled_expiry_dates,
        mrz_expiry_dates=sorted({item.value.isoformat() for item in mrz_evidence}),
        mrz_formats=sorted(
            {item.mrz_format for item in mrz_evidence if item.mrz_format}
        ),
        mrz_expiry_check_valid=(
            True
            if any(item.mrz_expiry_check_valid is True for item in mrz_evidence)
            else False
            if mrz_evidence
            else None
        ),
        existing_expiry_date=parsed_existing_expiry,
        matches_existing_expiry=matches_existing,
        review_reasons=review_reasons,
    )


def read_target_contact_documents(
    processed_document_ids: set[str],
    processed_contact_document_ids: set[str],
) -> list[dict]:
    table = Table("contact_document", db.metadata, autoload_with=db.engine)

    @db.with_session(commit=False)
    def _read(session):
        query = (
            session.query(table)
            .filter(table.c.category == CATEGORY)
            .order_by(table.c.created.desc(), table.c.id.desc())
        )
        selected = []
        selected_document_ids = set()
        for row in query.yield_per(500):
            payload = row._asdict()
            contact_document_id = str(payload.get("id") or "").strip()
            document_id = str(payload.get("document_id") or "").strip()
            if contact_document_id in processed_contact_document_ids:
                continue
            if document_id and (
                document_id in processed_document_ids
                or document_id in selected_document_ids
            ):
                continue
            selected.append(payload)
            if document_id:
                selected_document_ids.add(document_id)
            if len(selected) == TARGET_COUNT:
                break
        return selected

    return _read()


def read_raw_document(document_id: str) -> dict | None:
    rows = db.read(table="document", query={"id": document_id}) or []
    return rows[0] if rows else None


def _decode_raw_document(document_row: dict) -> bytes:
    encoded_data = str((document_row or {}).get("data") or "").strip()
    if not encoded_data:
        raise ValueError("document data is empty")
    try:
        return base64.b64decode(encoded_data)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("document data is not valid base64") from exc


DETAIL_FIELDS = [
    "contact_document_id",
    "document_id",
    "document_language",
    "type",
    "category",
    "mime_type",
    "existing_expiry_raw",
    "existing_expiry_date",
    "provider",
    "model_version",
    "pipeline_version",
    "status",
    "elapsed_seconds",
    "page_count",
    "region_count",
    "line_count",
    "word_count",
    "output_chars",
    "ocr_quality_score",
    "ocr_quality_status",
    "average_confidence",
    "provider_image_quality_score",
    "quality_reasons",
    "quality_defects",
    "expiry_extraction_status",
    "proposed_expiry_date",
    "expiry_confidence",
    "matches_existing_expiry",
    "candidate_conflict",
    "labeled_expiry_dates",
    "mrz_expiry_dates",
    "mrz_formats",
    "mrz_expiry_check_valid",
    "all_date_candidates",
    "expiry_candidate_details",
    "review_required",
    "review_priority",
    "review_reasons",
    "output_preview",
    "output_text",
    "error",
]

SUMMARY_FIELDS = [
    "provider",
    "batch_requested",
    "batch_selected",
    "cumulative_selected",
    "completed",
    "failed",
    "ocr_high_quality",
    "ocr_review_quality",
    "ocr_low_quality",
    "expiry_high_confidence",
    "expiry_needs_review",
    "expiry_not_found",
    "expiry_conflict",
    "matches_existing_expiry",
    "average_elapsed_seconds",
    "contact_document_updates",
    "document_processing_writes",
]


def load_existing_results() -> list[dict]:
    path = Path(DETAIL_CSV_PATH)
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames != DETAIL_FIELDS:
            raise ValueError(
                f"{DETAIL_CSV_PATH} has an incompatible header. "
                "Move or rename it before starting a new extraction history."
            )
        return list(reader)


def append_result(row: dict) -> None:
    path = Path(DETAIL_CSV_PATH)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=DETAIL_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _review_routing(
    status: str,
    quality_status: str,
    expiry_analysis: IDExpiryAnalysis,
) -> tuple[str, list[str]]:
    reasons = list(expiry_analysis.review_reasons)
    if status != "completed":
        reasons.append("ocr_failed")
    if quality_status != "high":
        reasons.append("ocr_quality_not_high")
    reasons = list(dict.fromkeys(reasons))
    if not reasons:
        reasons.append("pilot_manual_validation")
    priority = (
        "high"
        if status != "completed"
        or quality_status == "low"
        or expiry_analysis.status in {"not_found", "conflict"}
        else "normal"
    )
    return priority, reasons


def _failed_row(
    contact_document: dict,
    error: str,
    *,
    mime_type: str | None = None,
    elapsed_seconds: float = 0.0,
) -> dict:
    return {
        "contact_document_id": str(contact_document.get("id") or "").strip(),
        "document_id": str(contact_document.get("document_id") or "").strip(),
        "document_language": contact_document.get("document_language"),
        "type": contact_document.get("type"),
        "category": contact_document.get("category"),
        "mime_type": mime_type,
        "existing_expiry_raw": contact_document.get("expiry_date"),
        "existing_expiry_date": _parse_existing_expiry(
            contact_document.get("expiry_date")
        ),
        "provider": GOOGLE_DOCUMENT_AI_PROVIDER,
        "model_version": None,
        "pipeline_version": None,
        "status": "failed",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "page_count": 0,
        "region_count": 0,
        "line_count": 0,
        "word_count": 0,
        "output_chars": 0,
        "ocr_quality_score": 0,
        "ocr_quality_status": "low",
        "average_confidence": None,
        "provider_image_quality_score": None,
        "quality_reasons": "ocr_failed",
        "quality_defects": "[]",
        "expiry_extraction_status": "not_found",
        "proposed_expiry_date": None,
        "expiry_confidence": "none",
        "matches_existing_expiry": None,
        "candidate_conflict": False,
        "labeled_expiry_dates": "",
        "mrz_expiry_dates": "",
        "mrz_formats": "",
        "mrz_expiry_check_valid": None,
        "all_date_candidates": "",
        "expiry_candidate_details": "[]",
        "review_required": True,
        "review_priority": "high",
        "review_reasons": "ocr_failed | expiry_not_found",
        "output_preview": "<empty>",
        "output_text": "",
        "error": error,
    }


def _completed_row(
    contact_document: dict,
    mime_type: str,
    result,
    elapsed_seconds: float,
) -> dict:
    output_text = result.text.strip()
    quality = result.quality or assess_ocr_text(output_text)
    quality_payload = quality.as_dict()
    expiry_analysis = analyze_id_expiry(
        output_text,
        contact_document.get("expiry_date"),
        ocr_lines=[
            line.text
            for page in result.pages
            for line in page.lines
            if line.text
        ],
    )
    quality_status = str(quality_payload.get("status") or "low")
    review_priority, review_reasons = _review_routing(
        "completed",
        quality_status,
        expiry_analysis,
    )
    return {
        "contact_document_id": str(contact_document.get("id") or "").strip(),
        "document_id": str(contact_document.get("document_id") or "").strip(),
        "document_language": contact_document.get("document_language"),
        "type": contact_document.get("type"),
        "category": contact_document.get("category"),
        "mime_type": mime_type,
        "existing_expiry_raw": contact_document.get("expiry_date"),
        "existing_expiry_date": expiry_analysis.existing_expiry_date,
        "provider": result.provider,
        "model_version": result.model_version,
        "pipeline_version": result.pipeline_version,
        "status": "completed",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "page_count": len(result.pages),
        "region_count": sum(len(page.regions) for page in result.pages),
        "line_count": sum(len(page.lines) for page in result.pages),
        "word_count": sum(len(page.words) for page in result.pages),
        "output_chars": len(output_text),
        "ocr_quality_score": quality_payload.get("score"),
        "ocr_quality_status": quality_status,
        "average_confidence": quality_payload.get("average_confidence"),
        "provider_image_quality_score": quality_payload.get(
            "provider_quality_score"
        ),
        "quality_reasons": " | ".join(quality_payload.get("reasons") or []),
        "quality_defects": json.dumps(
            quality_payload.get("quality_defects") or [],
            ensure_ascii=False,
        ),
        "expiry_extraction_status": expiry_analysis.status,
        "proposed_expiry_date": expiry_analysis.proposed_expiry_date,
        "expiry_confidence": expiry_analysis.confidence,
        "matches_existing_expiry": expiry_analysis.matches_existing_expiry,
        "candidate_conflict": expiry_analysis.candidate_conflict,
        "labeled_expiry_dates": " | ".join(expiry_analysis.labeled_expiry_dates),
        "mrz_expiry_dates": " | ".join(expiry_analysis.mrz_expiry_dates),
        "mrz_formats": " | ".join(expiry_analysis.mrz_formats),
        "mrz_expiry_check_valid": expiry_analysis.mrz_expiry_check_valid,
        "all_date_candidates": " | ".join(expiry_analysis.all_date_candidates),
        "expiry_candidate_details": json.dumps(
            [candidate.as_dict() for candidate in expiry_analysis.candidates],
            ensure_ascii=False,
        ),
        "review_required": True,
        "review_priority": review_priority,
        "review_reasons": " | ".join(review_reasons),
        "output_preview": preview_text(output_text),
        "output_text": output_text,
        "error": None,
    }


def rewrite_results(rows: list[dict]) -> None:
    path = Path(DETAIL_CSV_PATH)
    temporary_path = path.with_suffix(".tmp")
    with temporary_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=DETAIL_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(path)


def reanalyze_existing_results(rows: list[dict]) -> int:
    changed_rows = 0
    for row in rows:
        output_text = str(row.get("output_text") or "").strip()
        if row.get("status") != "completed" or not output_text:
            continue
        analysis = analyze_id_expiry(
            output_text,
            row.get("existing_expiry_raw"),
        )
        review_priority, review_reasons = _review_routing(
            "completed",
            str(row.get("ocr_quality_status") or "low"),
            analysis,
        )
        updates = {
            "existing_expiry_date": analysis.existing_expiry_date,
            "expiry_extraction_status": analysis.status,
            "proposed_expiry_date": analysis.proposed_expiry_date,
            "expiry_confidence": analysis.confidence,
            "matches_existing_expiry": analysis.matches_existing_expiry,
            "candidate_conflict": analysis.candidate_conflict,
            "labeled_expiry_dates": " | ".join(
                analysis.labeled_expiry_dates
            ),
            "mrz_expiry_dates": " | ".join(analysis.mrz_expiry_dates),
            "mrz_formats": " | ".join(analysis.mrz_formats),
            "mrz_expiry_check_valid": analysis.mrz_expiry_check_valid,
            "all_date_candidates": " | ".join(analysis.all_date_candidates),
            "expiry_candidate_details": json.dumps(
                [candidate.as_dict() for candidate in analysis.candidates],
                ensure_ascii=False,
            ),
            "review_required": True,
            "review_priority": review_priority,
            "review_reasons": " | ".join(review_reasons),
        }
        if any(
            ("" if row.get(key) is None else str(row.get(key)))
            != ("" if value is None else str(value))
            for key, value in updates.items()
        ):
            changed_rows += 1
        row.update(updates)
    return changed_rows


def write_summary(summary: dict) -> None:
    with open(SUMMARY_CSV_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerow(summary)


def _is_true(value: object) -> bool:
    return value is True or str(value or "").strip().lower() == "true"


def summarize(rows: list[dict], batch_selected: int) -> dict:
    completed_rows = [row for row in rows if row["status"] == "completed"]
    elapsed_values = [
        float(row["elapsed_seconds"])
        for row in completed_rows
    ]
    return {
        "provider": GOOGLE_DOCUMENT_AI_PROVIDER,
        "batch_requested": TARGET_COUNT,
        "batch_selected": batch_selected,
        "cumulative_selected": len(rows),
        "completed": len(completed_rows),
        "failed": len(rows) - len(completed_rows),
        "ocr_high_quality": sum(
            row["ocr_quality_status"] == "high" for row in rows
        ),
        "ocr_review_quality": sum(
            row["ocr_quality_status"] == "review" for row in rows
        ),
        "ocr_low_quality": sum(
            row["ocr_quality_status"] == "low" for row in rows
        ),
        "expiry_high_confidence": sum(
            row["expiry_extraction_status"] == "high" for row in rows
        ),
        "expiry_needs_review": sum(
            row["expiry_extraction_status"] == "review" for row in rows
        ),
        "expiry_not_found": sum(
            row["expiry_extraction_status"] == "not_found" for row in rows
        ),
        "expiry_conflict": sum(
            row["expiry_extraction_status"] == "conflict" for row in rows
        ),
        "matches_existing_expiry": sum(
            _is_true(row["matches_existing_expiry"]) for row in rows
        ),
        "average_elapsed_seconds": (
            round(sum(elapsed_values) / len(elapsed_values), 3)
            if elapsed_values
            else 0
        ),
        "contact_document_updates": 0,
        "document_processing_writes": 0,
    }


def main() -> None:
    validate_ocr_provider_configuration(GOOGLE_DOCUMENT_AI_PROVIDER)
    existing_results = load_existing_results()
    reanalyzed_rows = reanalyze_existing_results(existing_results)
    if reanalyzed_rows:
        rewrite_results(existing_results)
        print(f"reanalyzed_existing_rows: {reanalyzed_rows}")
    processed_document_ids = {
        str(row.get("document_id") or "").strip()
        for row in existing_results
        if str(row.get("document_id") or "").strip()
    }
    processed_contact_document_ids = {
        str(row.get("contact_document_id") or "").strip()
        for row in existing_results
        if str(row.get("contact_document_id") or "").strip()
    }
    contact_documents = read_target_contact_documents(
        processed_document_ids,
        processed_contact_document_ids,
    )
    if not contact_documents:
        summary = summarize(existing_results, batch_selected=0)
        write_summary(summary)
        print("No unprocessed Proof of Identity documents remain.")
        print(summary)
        return

    print("=== Google Document AI OCR batch ===")
    print(f"category: {CATEGORY}")
    print(f"previously_processed: {len(existing_results)}")
    print(f"batch_requested: {TARGET_COUNT}")
    print(f"batch_selected: {len(contact_documents)}")
    print("database_writes: disabled")

    batch_rows = []

    def record_result(row: dict) -> None:
        append_result(row)
        existing_results.append(row)
        batch_rows.append(row)

    for index, contact_document in enumerate(contact_documents, start=1):
        document_id = str(contact_document.get("document_id") or "").strip()
        print(f"\n[{index}/{len(contact_documents)}] document_id={document_id or '<missing>'}")

        if not document_id:
            row = _failed_row(
                contact_document,
                "contact_document has no document_id",
            )
            record_result(row)
            print(f"status=failed error={row['error']}")
            continue

        document_row = read_raw_document(document_id)
        if document_row is None:
            row = _failed_row(
                contact_document,
                f"document not found: {document_id}",
            )
            record_result(row)
            print(f"status=failed error={row['error']}")
            continue

        mime_type = str(document_row.get("mime_type") or "").strip().lower()
        try:
            file_bytes = _decode_raw_document(document_row)
        except ValueError as exc:
            row = _failed_row(
                contact_document,
                str(exc),
                mime_type=mime_type,
            )
            record_result(row)
            print(f"status=failed error={row['error']}")
            continue

        started = perf_counter()
        try:
            result = extract_document_ocr(
                file_bytes,
                source_language=contact_document.get("document_language"),
                mime_type=mime_type,
                provider=GOOGLE_DOCUMENT_AI_PROVIDER,
                render_dpi=OCR_RENDER_DPI,
                max_render_dimension=OCR_MAX_RENDER_DIMENSION,
                use_cache=True,
            )
            row = _completed_row(
                contact_document,
                mime_type,
                result,
                perf_counter() - started,
            )
            print(
                f"status=completed seconds={row['elapsed_seconds']:.3f} "
                f"quality={row['ocr_quality_status']} "
                f"expiry={row['expiry_extraction_status']}:"
                f"{row['proposed_expiry_date'] or '-'}"
            )
        except Exception as exc:
            row = _failed_row(
                contact_document,
                str(exc),
                mime_type=mime_type,
                elapsed_seconds=perf_counter() - started,
            )
            print(
                f"status=failed seconds={row['elapsed_seconds']:.3f} "
                f"error={row['error']}"
            )
        record_result(row)

    summary = summarize(existing_results, batch_selected=len(batch_rows))
    write_summary(summary)

    print("\n=== batch summary ===")
    print(summary)
    print(f"detail_csv: {DETAIL_CSV_PATH}")
    print(f"summary_csv: {SUMMARY_CSV_PATH}")


if __name__ == "__main__":
    main()
