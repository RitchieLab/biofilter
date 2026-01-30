from __future__ import annotations

"""
PostgreSQL-backed Text Miner (pg_trgm)

This backend is optimized for PostgreSQL using the pg_trgm extension and a GIN
index on `entity_aliases.alias_norm` (and optionally alias_value).

Goal:
- Given a potentially large free-text document, recall entity alias candidates
  efficiently using trigram similarity.
- Convert candidates into *mentions* with approximate spans in the original text.
- Return mentions with ranked candidates, leaving final "relationship building"
  to downstream consumers (reports/DTPs).

Important notes:
- This module depends on:
    - SQLAlchemy Session
    - pg_trgm functions/operators (similarity, %)
    - Biofilter Entity tables (Entity, EntityAlias)
    - The generic contract in text_miner.py
- Span detection is best-effort:
    - We attempt case-insensitive substring matches in raw text using alias_value
      (preferred) and alias_norm (fallback).
    - If not found, we fallback to the chunk window span itself.
"""

from dataclasses import dataclass
import re
from typing import Iterable, Optional

from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session, aliased

from biofilter.modules.db.models.model_entities import Entity, EntityAlias
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
# Backend-specific config (kept out of TextMinerConfig by design)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PgTextMinerConfig:
    """
    pg_trgm backend tuning parameters.

    - similarity_threshold: minimum pg_trgm similarity to keep a candidate.
      Typical values:
        * 0.2–0.3 for higher recall
        * 0.35–0.5 for precision
    - per_query_limit: max rows returned per pg_trgm query (per window).
    - max_windows_per_chunk: safety cap to avoid too many queries on long chunks.
    """
    similarity_threshold: float = 0.30
    per_query_limit: int = 200
    max_windows_per_chunk: int = 80

    # Token-window generation:
    # - for best results, generate 3..8 token windows from normalized strict tokens.
    min_window_tokens: int = 3
    max_window_tokens: int = 8

    # If True, also run a single query for the entire chunk strict text.
    include_chunk_query: bool = True

    # Controls whether we try alias_value and alias_norm for similarity.
    # Typically alias_norm is the best choice.
    use_alias_value_similarity: bool = False


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _iter_chunk_windows(
    strict_text: str,
    tokens: tuple[str, ...],
    *,
    cfg: PgTextMinerConfig,
) -> Iterable[str]:
    """
    Generate query windows for pg_trgm matching.
    """
    if cfg.include_chunk_query and strict_text:
        yield strict_text

    toks = [t for t in tokens if t]
    if not toks:
        return

    max_w = min(cfg.max_window_tokens, len(toks))
    min_w = min(cfg.min_window_tokens, max_w)

    count = 0
    for w in range(max_w, min_w - 1, -1):
        for i in range(0, len(toks) - w + 1):
            window = " ".join(toks[i : i + w]).strip()
            if not window:
                continue
            yield window
            count += 1
            if count >= cfg.max_windows_per_chunk:
                return


def _is_postgres(session: Session) -> bool:
    try:
        return session.bind.dialect.name == "postgresql"  # type: ignore[union-attr]
    except Exception:
        return False


def _find_spans_case_insensitive(haystack: str, needle: str) -> list[tuple[int, int]]:
    """
    Return all [start,end) spans of needle in haystack, case-insensitive.
    Uses regex escape to avoid special chars acting as regex operators.
    """
    if not needle or len(needle) < 2:
        return []

    try:
        pattern = re.compile(re.escape(needle), re.IGNORECASE)
    except re.error:
        return []

    return [(m.start(), m.end()) for m in pattern.finditer(haystack)]


def _best_effort_span(
    raw_text: str,
    *,
    chunk_start: int,
    chunk_end: int,
    alias_value: Optional[str],
    alias_norm: Optional[str],
) -> list[Span]:
    """
    Try to locate the alias in the ORIGINAL raw text; if not found, return a span
    covering the chunk window (as a fallback).

    Strategy:
    1) Find alias_value (case-insensitive) in raw_text
    2) Else find alias_norm (case-insensitive) in raw_text
    3) Else fallback to chunk span
    """
    spans: list[Span] = []

    # Prefer alias_value for offsets (it matches what user would see).
    if alias_value:
        for s, e in _find_spans_case_insensitive(raw_text, alias_value):
            spans.append(Span(start=s, end=e, text=raw_text[s:e]))
        if spans:
            return spans

    # Fallback to alias_norm
    if alias_norm:
        for s, e in _find_spans_case_insensitive(raw_text, alias_norm):
            spans.append(Span(start=s, end=e, text=raw_text[s:e]))
        if spans:
            return spans

    # Fallback to chunk span
    cs = max(0, chunk_start)
    ce = min(len(raw_text), chunk_end)
    if ce > cs:
        spans.append(Span(start=cs, end=ce, text=raw_text[cs:ce]))
    return spans


# -----------------------------------------------------------------------------
# Main backend
# -----------------------------------------------------------------------------
class PgTrgmTextMiner(BaseTextMiner):
    """
    PostgreSQL pg_trgm text miner.

    This class performs:
    - chunking over the input text
    - pg_trgm candidate retrieval on alias_norm (and optionally alias_value)
    - best-effort span localization in original text
    - shared post-processing (longest-span-wins, dedup, caps)
    """

    name = "pg_trgm"

    def __init__(
        self,
        session: Session,
        *,
        normalizer: Optional[TextNormalizer] = None,
        cfg: Optional[PgTextMinerConfig] = None,
        require_active_alias: bool = True,
        require_active_entity: bool = True,
    ):
        self.session = session
        self.normalizer = normalizer or TextNormalizer()
        self.cfg = cfg or PgTextMinerConfig()
        self.require_active_alias = require_active_alias
        self.require_active_entity = require_active_entity

        if not _is_postgres(session):
            raise RuntimeError(
                "PgTrgmTextMiner requires a PostgreSQL engine/bind. "
                "Use the fallback miner for SQLite."
            )

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

        mentions_map: dict[tuple[int, int], Mention] = {}
        stats = {
            "chunks": 0,
            "windows": 0,
            "pg_queries": 0,
            "rows": 0,
            "candidates": 0,
            "spans": 0,
        }

        try:
            # Iterate chunks as windows over characters (backend-agnostic)
            for chunk_start, chunk_end, chunk_text in self._iter_chunks(raw_text, cfg):
                stats["chunks"] += 1

                # Normalize chunk into strict + tokens
                nq = self.normalizer.build(chunk_text)

                # Generate pg_trgm query windows
                windows = list(
                    _iter_chunk_windows(
                        nq.strict or "",
                        nq.tokens,
                        cfg=self.cfg,
                    )
                )
                stats["windows"] += len(windows)

                # For each window, pull candidates via pg_trgm
                for qwin in windows:
                    rows = self._pg_trgm_query(qwin, cfg=cfg)
                    stats["pg_queries"] += 1
                    stats["rows"] += len(rows)

                    for r in rows:
                        # Locate mention spans in original text
                        alias_value = r.alias_value
                        alias_norm = r.alias_norm

                        spans = _best_effort_span(
                            raw_text,
                            chunk_start=chunk_start,
                            chunk_end=chunk_end,
                            alias_value=alias_value,
                            alias_norm=alias_norm,
                        )
                        stats["spans"] += len(spans)

                        cand = MentionCandidate(
                            entity_id=int(r.entity_id),
                            group_id=int(r.group_id) if r.group_id is not None else None,
                            entity_type=str(r.group_id) if r.group_id is not None else None,
                            primary_name=r.primary_name,
                            matched_name=r.alias_norm or r.alias_value,
                            matched_name_id=int(r.alias_id) if r.alias_id is not None else None,
                            method="pg_trgm",
                            # We convert pg similarity (0..1) to 0..100 to align with resolver scores.
                            score=float(r.pg_trgm_score) * 100.0,
                            data_source=str(r.data_source_id) if r.data_source_id is not None else None,
                            meta={
                                "group_id": int(r.group_id) if r.group_id is not None else None,
                                "alias_type": r.alias_type,
                                "xref_source": r.xref_source,
                                "locale": r.locale,
                                "is_primary_name": bool(r.is_primary),
                                "etl_package_id": r.etl_package_id,
                                "alias_value": r.alias_value,
                                "alias_norm": r.alias_norm,
                                "pg_trgm_score": float(r.pg_trgm_score),
                            },
                        )
                        stats["candidates"] += 1

                        for sp in spans:
                            key = (sp.start, sp.end)
                            m = mentions_map.get(key)
                            if m is None:
                                m = Mention(span=sp, candidates=[], best=None)
                                mentions_map[key] = m
                            m.candidates.append(cand)

            # Finalize + shared post-processing
            mentions = list(mentions_map.values())
            mentions = postprocess_mentions(mentions, config=cfg)

            # Optional: drop weak mentions if keep_ambiguous=False
            if not cfg.keep_ambiguous:
                strong: list[Mention] = []
                for m in mentions:
                    m.finalize()
                    if m.best and m.best.score >= cfg.min_score:
                        strong.append(m)
                mentions = strong

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
    def _iter_chunks(self, text: str, cfg: TextMinerConfig) -> Iterable[tuple[int, int, str]]:
        """
        Yield (start, end, chunk_text) over the raw text.

        Windowing approach (char-based) is robust and DB-backend agnostic.
        Sentence-based chunking can be added later (pluggable chunker).
        """
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
    def _pg_trgm_query(self, query_text: str, *, cfg: TextMinerConfig):
        """
        Perform one pg_trgm similarity query and return rows.

        We prioritize alias_norm for matching because it is designed for search.
        We also optionally enable alias_value similarity (rarely needed).
        """
        PrimaryAlias = aliased(EntityAlias)

        # Base filters (shared knobs)
        filters = []
        if self.require_active_alias:
            filters.append(EntityAlias.is_active.is_(True))
        if self.require_active_entity:
            filters.append(Entity.is_active.is_(True))
        if cfg.locale:
            filters.append(EntityAlias.locale == cfg.locale)
        if cfg.data_source_ids:
            filters.append(EntityAlias.data_source_id.in_(cfg.data_source_ids))

        # Group filter via entity_type_hints: allow either names or ids as strings
        # Here we only support numeric group ids (or pre-resolved from outside).
        # (You can enhance: map group names -> ids, similar to db_retriever.)
        group_ids: list[int] = []
        if cfg.entity_type_hints:
            for h in cfg.entity_type_hints:
                if not h:
                    continue
                key = str(h).strip()
                if key.isdigit():
                    group_ids.append(int(key))
        if group_ids:
            filters.append(EntityAlias.group_id.in_(group_ids))

        # Similarity expression
        sim_norm = func.similarity(EntityAlias.alias_norm, query_text)
        # pg_trgm operator: alias_norm % query_text (uses current similarity_threshold)
        # We still add an explicit similarity >= threshold for determinism.
        trigram_match_norm = EntityAlias.alias_norm.op("%")(query_text)

        where_clause = or_(
            trigram_match_norm,
            sim_norm >= self.cfg.similarity_threshold,
        )

        # Optional: also consider alias_value similarity (usually not necessary)
        if self.cfg.use_alias_value_similarity:
            sim_val = func.similarity(EntityAlias.alias_value, query_text)
            trigram_match_val = EntityAlias.alias_value.op("%")(query_text)
            where_clause = or_(
                where_clause,
                trigram_match_val,
                sim_val >= self.cfg.similarity_threshold,
            )
            sim_expr = func.greatest(sim_norm, sim_val).label("pg_trgm_score")
        else:
            sim_expr = sim_norm.label("pg_trgm_score")

        stmt = (
            select(
                EntityAlias.id.label("alias_id"),
                EntityAlias.entity_id.label("entity_id"),
                EntityAlias.group_id.label("group_id"),
                EntityAlias.alias_value.label("alias_value"),
                EntityAlias.alias_norm.label("alias_norm"),
                EntityAlias.alias_type.label("alias_type"),
                EntityAlias.xref_source.label("xref_source"),
                EntityAlias.is_primary.label("is_primary"),
                EntityAlias.locale.label("locale"),
                EntityAlias.data_source_id.label("data_source_id"),
                EntityAlias.etl_package_id.label("etl_package_id"),
                PrimaryAlias.alias_value.label("primary_name"),
                sim_expr,
            )
            .select_from(EntityAlias)
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .outerjoin(
                PrimaryAlias,
                (PrimaryAlias.entity_id == Entity.id) & (PrimaryAlias.is_primary.is_(True)),
            )
            .where(*filters)
            .where(where_clause)
            .order_by(
                sim_expr.desc(),
                EntityAlias.is_primary.desc(),
                EntityAlias.id.asc(),
            )
            .limit(self.cfg.per_query_limit)
        )

        return self.session.execute(stmt).all()
