# utils/text_normalizer.py

import re
import html
import unicodedata
from typing import Optional


WHITESPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b", re.IGNORECASE)


def strip_accents(text: str) -> str:
    """
    Remove accents/diacritics from text.
    Example: 'Türkiye' -> 'Turkiye'
    """
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_quotes(text: str) -> str:
    """
    Normalize curly/special quotes and dashes to simpler ASCII variants.
    """
    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u00a0": " ",  # non-breaking space
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def remove_urls(text: str) -> str:
    return URL_RE.sub(" ", text)


def remove_emails(text: str) -> str:
    return EMAIL_RE.sub(" ", text)


def collapse_whitespace(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text).strip()


def basic_clean(text: Optional[str]) -> str:
    """
    Basic safe text cleanup without aggressive transformations.
    Keeps meaning intact.
    """
    if not text:
        return ""

    text = html.unescape(text)
    text = normalize_quotes(text)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = collapse_whitespace(text)
    return text


def normalize_text(
    text: Optional[str],
    *,
    lowercase: bool = True,
    remove_links: bool = False,
    remove_email_addresses: bool = False,
    strip_diacritics: bool = False,
) -> str:
    """
    Main normalization function used across the pipeline.
    """
    cleaned = basic_clean(text)

    if remove_links:
        cleaned = remove_urls(cleaned)

    if remove_email_addresses:
        cleaned = remove_emails(cleaned)

    cleaned = collapse_whitespace(cleaned)

    if strip_diacritics:
        cleaned = strip_accents(cleaned)

    if lowercase:
        cleaned = cleaned.lower()

    return cleaned


def build_searchable_text(*parts: Optional[str]) -> str:
    """
    Merge multiple text fragments into one normalized searchable string.
    Useful for title + summary + body.
    """
    merged = " ".join(part for part in parts if part)
    return normalize_text(
        merged,
        lowercase=True,
        remove_links=True,
        remove_email_addresses=True,
        strip_diacritics=False,
    )
