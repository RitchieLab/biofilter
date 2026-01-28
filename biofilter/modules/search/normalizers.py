from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

from biofilter.modules.search.types import NormalizedQuery


_GREEK_MAP = {
    "α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta",
    "κ": "kappa", "λ": "lambda", "μ": "mu", "ω": "omega",
}

_PUNCT_RE = re.compile(r"[^\w\s]+", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class NormalizerConfig:
    """Deterministic normalization rules."""
    ascii_fold: bool = True
    greek_letters: bool = True
    remove_punct: bool = True
    collapse_spaces: bool = True
    lowercase: bool = True
    strip: bool = True
    min_token_len: int = 2


class TextNormalizer:
    def __init__(self, config: NormalizerConfig | None = None):
        self.config = config or NormalizerConfig()

    def normalize_basic(self, text: str) -> str:
        t = text or ""
        if self.config.strip:
            t = t.strip()
        if self.config.lowercase:
            t = t.lower()

        # Unicode normalization
        t = unicodedata.normalize("NFKC", t)

        if self.config.greek_letters:
            for k, v in _GREEK_MAP.items():
                t = t.replace(k, v)

        if self.config.collapse_spaces:
            t = _WS_RE.sub(" ", t).strip()

        return t

    def normalize_strict(self, text: str) -> str:
        t = self.normalize_basic(text)

        if self.config.remove_punct:
            t = _PUNCT_RE.sub(" ", t)

        if self.config.ascii_fold:
            # Strip accents/diacritics
            t = "".join(
                ch for ch in unicodedata.normalize("NFKD", t)
                if not unicodedata.combining(ch)
            )

        if self.config.collapse_spaces:
            t = _WS_RE.sub(" ", t).strip()

        return t

    def tokens(self, text: str) -> tuple[str, ...]:
        t = self.normalize_strict(text)
        parts = [p for p in t.split(" ") if len(p) >= self.config.min_token_len]
        return tuple(parts)

    def build(self, text: str) -> NormalizedQuery:
        basic = self.normalize_basic(text)
        strict = self.normalize_strict(text)
        toks = self.tokens(text)
        return NormalizedQuery(raw=text, basic=basic, strict=strict, tokens=toks)


def iter_unique_tokens(tokens: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out
