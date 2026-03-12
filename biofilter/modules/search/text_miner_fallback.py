from __future__ import annotations

"""
Portable Text Miner (Fallback)
------------------------------

This backend works on any database (SQLite/Postgres/etc.) and does NOT require
pg_trgm.

Strategy:
1) Chunk the text (char windows)
2) Generate candidate spans using a lightweight n-gram windowing over tokens
3) For each candidate span, call TermResolver.resolve_best() (or .search()) to
   ground the span to an Entity via the Search Engine
4) Keep mentions that are:
   - resolved (>= min_score), OR
   - ambiguous/weak if keep_ambiguous=True and candidates exist
5) Post-process overlaps (longest-span-wins) and optional dedup.

Notes:
- This is slower than pg_trgm for large documents, but portable.
- Tune n-gram sizes for Biofilter domains:
  * Chemicals: 3..10 token windows
  * Diseases: 2..8
  * Genes/Proteins: 1..4 (symbols/codes)
- For speed, this miner uses:
  * aggressive pruning: skip short/stopword-only windows
  * LRU cache for resolver calls per normalized window string
"""

from dataclasses import dataclass
from functools import lru_cache
import re
from typing import Iterable, Optional, Sequence

from biofilter.modules.search.resolver import TermResolver
from biofilter.modules.search.normalizers import TextNormalizer

from biofilter.modules.search.text_miner import (
    BaseTextMiner,
    Mention,
    MentionCandidate,
    MiningResult,
    Span,
    TextMinerConfig,
    clamp_text,
    postprocess_mentions,
)


# -----------------------------------------------------------------------------
# Backend-specific config
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class FallbackTextMinerConfig:
    """
    n-gram fallback tuning parameters.

    - min_ngram/max_ngram: token-window sizes to evaluate
    - max_windows_per_chunk: safety cap (prevents O(n^2) blow-ups)
    - use_resolver_search: if True, use resolver.search() for top_k candidates;
      else use resolve_best() and keep only best.
    """

    min_ngram: int = 2
    max_ngram: int = 8
    max_windows_per_chunk: int = 2500

    # If True, call resolver.search(span, limit=top_k) and build candidates list.
    # If False, call resolver.resolve_best() and keep only the best candidate.
    use_resolver_search: bool = True

    # Skip tokens that are too short / too noisy
    min_token_len: int = 2
    allow_numeric: bool = True

    # Simple stopwords (can be expanded later)
    stopwords: frozenset[str] = frozenset(
        {
            "a",
            "an",
            "and",
            "or",
            "the",
            "of",
            "to",
            "in",
            "on",
            "for",
            "with",
            "by",
            "from",
            "at",
            "as",
            "is",
            "are",
            "was",
            "were",
            "be",
            "been",
        }
    )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
_WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-\:\_\/]*")


def _tokenize_with_offsets(text: str) -> list[tuple[str, int, int]]:
    """
    Return [(token, start, end), ...] where start/end are char offsets in raw text.
    Uses a conservative regex to keep codes like HGNC:5, ENSG..., A1BG-AS1, etc.
    """
    out: list[tuple[str, int, int]] = []
    for m in _WORD_RE.finditer(text):
        tok = m.group(0)
        out.append((tok, m.start(), m.end()))
    return out


def _iter_ngram_windows(
    toks: Sequence[tuple[str, int, int]],
    *,
    min_n: int,
    max_n: int,
    cap: int,
) -> Iterable[tuple[str, int, int]]:
    """
    Yield (window_text, start_char, end_char) for token n-grams.

    We join tokens with a single space to build the query string.
    Span offsets cover from first token start to last token end in raw text.
    """
    n_tokens = len(toks)
    if n_tokens == 0:
        return

    max_n = min(max_n, n_tokens)
    min_n = min(min_n, max_n)

    count = 0
    # Prefer longer windows first (often better for chemicals/diseases)
    for n in range(max_n, min_n - 1, -1):
        for i in range(0, n_tokens - n + 1):
            window_tokens = toks[i : i + n]
            s = window_tokens[0][1]
            e = window_tokens[-1][2]
            text_window = " ".join(t[0] for t in window_tokens)
            yield (text_window, s, e)
            count += 1
            if count >= cap:
                return


def _looks_like_noise(window: str, *, cfg: FallbackTextMinerConfig) -> bool:
    w = window.strip()
    if not w:
        return True
    low = w.lower()

    # single stopword window
    if low in cfg.stopwords:
        return True

    # all stopwords
    parts = [p.lower() for p in low.split()]
    if parts and all(p in cfg.stopwords for p in parts):
        return True

    # too short overall
    if len(w) < 3:
        return True

    # numeric-only windows can be noisy
    if not cfg.allow_numeric:
        if all(ch.isdigit() or ch.isspace() for ch in w):
            return True

    return False


# -----------------------------------------------------------------------------
# Main backend
# -----------------------------------------------------------------------------
class NgramFallbackTextMiner(BaseTextMiner):
    """
    Portable n-gram + TermResolver miner.

    You provide a fully-configured TermResolver (which can use:
    - Postgres retriever + rapidfuzz
    - SQLite retriever + rapidfuzz
    etc.)

    This miner is intentionally conservative and uses caps to avoid slow runs.
    """

    name = "fallback_ngram"

    def __init__(
        self,
        resolver: TermResolver,
        *,
        normalizer: Optional[TextNormalizer] = None,
        cfg: Optional[FallbackTextMinerConfig] = None,
    ):
        self.resolver = resolver
        self.normalizer = normalizer or TextNormalizer()
        self.cfg = cfg or FallbackTextMinerConfig()

    # ---------------------------------------------------------------------
    def extract_mentions(
        self,
        text: str,
        *,
        config: Optional[TextMinerConfig] = None,
    ) -> MiningResult:
        cfg = config or TextMinerConfig()
        raw_text = clamp_text(text or "", cfg.max_text_chars)

        if not raw_text.strip():
            return MiningResult(
                status="empty",
                text_len=len(raw_text),
                mentions=[],
                backend=self.name,
                reason="Input text is empty/blank.",
            )

        stats = {
            "chunks": 0,
            "tokens": 0,
            "windows": 0,
            "resolver_calls": 0,
            "kept_mentions": 0,
        }

        mentions: list[Mention] = []

        try:
            for chunk_start, chunk_end, chunk_text in self._iter_chunks(raw_text, cfg):
                stats["chunks"] += 1

                toks = _tokenize_with_offsets(chunk_text)
                stats["tokens"] += len(toks)

                # Convert chunk-local offsets to global offsets
                toks_global = [
                    (t, s + chunk_start, e + chunk_start) for (t, s, e) in toks
                ]

                for window_text, s, e in _iter_ngram_windows(
                    toks_global,
                    min_n=self.cfg.min_ngram,
                    max_n=self.cfg.max_ngram,
                    cap=self.cfg.max_windows_per_chunk,
                ):
                    stats["windows"] += 1

                    if _looks_like_noise(window_text, cfg=self.cfg):
                        continue

                    # Resolve
                    res = self._resolve_cached(
                        window_text, tuple(cfg.entity_type_hints or ())
                    )
                    stats["resolver_calls"] += 1

                    # Decide keep/drop
                    if res["status"] == "not_found":
                        continue

                    # Build mention candidates
                    cands: list[MentionCandidate] = res["candidates"]
                    if not cands:
                        continue

                    best = cands[0]
                    if best.score < cfg.min_score and not cfg.keep_ambiguous:
                        continue

                    sp = Span(start=s, end=e, text=raw_text[s:e])
                    m = Mention(span=sp, candidates=cands, best=best)
                    mentions.append(m)

            mentions = postprocess_mentions(mentions, config=cfg)

            stats["kept_mentions"] = len(mentions)

            return MiningResult(
                status="ok",
                text_len=len(raw_text),
                mentions=mentions,
                backend=self.name,
                reason=None,
                stats=stats,
            )

        except Exception as e:
            return MiningResult(
                status="error",
                text_len=len(raw_text),
                mentions=[],
                backend=self.name,
                reason=f"{type(e).__name__}: {e}",
                stats=stats,
            )

    # ---------------------------------------------------------------------
    def _iter_chunks(
        self, text: str, cfg: TextMinerConfig
    ) -> Iterable[tuple[int, int, str]]:
        n = len(text)
        if cfg.chunk_size <= 0 or n <= cfg.chunk_size:
            yield (0, n, text)
            return

        step = max(1, cfg.chunk_size - max(0, cfg.chunk_overlap))
        start = 0
        while start < n:
            end = min(n, start + cfg.chunk_size)
            yield (start, end, text[start:end])
            if end >= n:
                break
            start += step

    # ---------------------------------------------------------------------
    @lru_cache(maxsize=100_000)
    def _resolve_cached(
        self, window_text: str, entity_type_hints: tuple[str, ...]
    ) -> dict:
        """
        Cache resolver calls by window string + type hints.

        Returns a small dict with:
        - status: "resolved"|"ambiguous"|"not_found"
        - candidates: list[MentionCandidate] (best-first)
        """
        hints = list(entity_type_hints) if entity_type_hints else None

        if self.cfg.use_resolver_search:
            # Get top-k candidates and translate into MentionCandidate
            results = self.resolver.search(
                window_text, entity_type_hints=hints, limit=5
            )
            if not results:
                return {"status": "not_found", "candidates": []}

            cands: list[MentionCandidate] = []
            for c in results:
                cands.append(
                    MentionCandidate(
                        entity_id=int(c.entity_id),
                        group_id=(
                            int(c.meta.get("group_id"))
                            if c.meta.get("group_id") is not None
                            else None
                        ),
                        entity_type=c.entity_type,
                        primary_name=c.primary_name,
                        matched_name=c.matched_name,
                        matched_name_id=(
                            int(c.matched_name_id)
                            if c.matched_name_id is not None
                            else None
                        ),
                        method=c.method or "ngram",
                        score=float(c.score),
                        data_source=c.data_source,
                        meta=dict(c.meta or {}),
                    )
                )
            # Status heuristic based on best score
            status = "resolved" if cands[0].score >= 90.0 else "ambiguous"
            return {"status": status, "candidates": cands}

        # Otherwise use resolve_best
        res = self.resolver.resolve_best(window_text, entity_type_hints=hints)
        if res.status == "not_found" and (res.best is None or res.best.score < 1.0):
            return {"status": "not_found", "candidates": []}

        cands: list[MentionCandidate] = []
        if res.best:
            b = res.best
            cands.append(
                MentionCandidate(
                    entity_id=int(b.entity_id),
                    group_id=(
                        int(b.meta.get("group_id"))
                        if b.meta.get("group_id") is not None
                        else None
                    ),
                    entity_type=b.entity_type,
                    primary_name=b.primary_name,
                    matched_name=b.matched_name,
                    matched_name_id=(
                        int(b.matched_name_id)
                        if b.matched_name_id is not None
                        else None
                    ),
                    method=b.method or "ngram",
                    score=float(b.score),
                    data_source=b.data_source,
                    meta=dict(b.meta or {}),
                )
            )

        return {"status": res.status, "candidates": cands}
