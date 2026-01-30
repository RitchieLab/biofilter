from __future__ import annotations

"""
PostgreSQL-backed Text Miner (pg_trgm)

Key fix:
- Prefer token-targeted mention mining (surface-form extraction) over
  chunk-window similarity queries.

Why:
- Chunk-window pg_trgm queries often recall generic synonyms in the DB
  (e.g., "multiple ..."), causing false positives and slowdowns.
- For domains like Genes and IDs, we want precise spans and exact/near-exact
  matching on short surface forms (A1BG, HGNC:5, ENSG..., etc.).

Design:
1) Extract "surface forms" from raw text with spans (regex heuristics).
2) For each surface form:
   - try exact match (alias_value / alias_norm)
   - fallback to pg_trgm similarity on alias_norm
3) Optional: if desired, run chunk-window pg_trgm queries as a last fallback.
"""

from dataclasses import dataclass
import re
from typing import Iterable, Optional

from sqlalchemy import select, func, or_, and_
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
    """
    similarity_threshold: float = 0.30
    per_query_limit: int = 200
    max_windows_per_chunk: int = 80

    # Token-window generation (legacy fallback)
    min_window_tokens: int = 3
    max_window_tokens: int = 8
    include_chunk_query: bool = False  # IMPORTANT: disable by default

    # Similarity field
    use_alias_value_similarity: bool = False

    # NEW: Prefer surface-form mining (recommended)
    prefer_surface_forms: bool = True
    max_surface_forms_per_chunk: int = 500

    # If True, and a surface form yields an exact hit, we can skip pg_trgm for it.
    short_circuit_on_exact: bool = True


# -----------------------------------------------------------------------------
# Surface form extraction
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class SurfaceForm:
    text: str
    start: int  # absolute start in full raw text
    end: int    # absolute end in full raw text


_STOPWORDS_GENERIC = {
    "the", "and", "or", "of", "in", "on", "to", "for", "with", "by",
    "this", "that", "these", "those", "paper", "study", "assay", "assays",
    "gene", "genes", "protein", "proteins", "pathway", "pathways",
    "multiple", "mutated", "factor", "group", "domain",
}


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
    Legacy pg_trgm windows (kept as optional fallback).
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


def _extract_surface_forms(
    chunk_text: str,
    *,
    chunk_start: int,
    cfg: TextMinerConfig,
    max_forms: int,
) -> list[SurfaceForm]:
    """
    Heuristic surface-form extractor.

    Goals:
    - High precision for IDs (HGNC:xxx, ENSG..., CHEBI:xxx, etc.)
    - Good recall for gene-like symbols (A1BG, BRCA1, MTSS1, etc.)
    - Return spans in absolute coordinates in the full raw text.

    Notes:
    - You can extend patterns per domain later (proteins, pathways, etc.)
    - For now we keep it generic + ID heavy.
    """
    text = chunk_text
    forms: list[SurfaceForm] = []

    # ID-like patterns (very strong signals)
    id_patterns = [
        r"\bHGNC:\d+\b",
        r"\bENSG\d{6,}\b",
        r"\bCHEBI:\d+\b",
        r"\bGO:\d+\b",
        r"\bREACTOME:\d+\b",
        r"\bR-HSA-\d+\b",     # Reactome stable IDs (common)
        r"\bP\d{5}\b",        # Uniprot-ish (very rough; keep conservative)
        r"\bQ\d{5}\b",
    ]

    for pat in id_patterns:
        for m in re.finditer(pat, text):
            s, e = m.start(), m.end()
            raw = text[s:e]
            if len(raw) >= cfg.min_span_chars:
                forms.append(SurfaceForm(raw, chunk_start + s, chunk_start + e))
                if len(forms) >= max_forms:
                    return forms

    # Gene-symbol-like patterns (heuristic)
    # - uppercase/digit/hyphen tokens with length >= 3
    # - prefer tokens that contain at least one digit OR are all-caps
    gene_like_pat = r"\b[A-Za-z0-9][A-Za-z0-9\-]{2,}\b"
    for m in re.finditer(gene_like_pat, text):
        s, e = m.start(), m.end()
        raw = text[s:e]
        low = raw.lower()

        if low in _STOPWORDS_GENERIC:
            continue
        if len(raw) < cfg.min_span_chars:
            continue

        has_digit = any(ch.isdigit() for ch in raw)
        is_all_caps = raw.isupper()

        # Avoid normal words (e.g., "multiple", "assays")
        if not (has_digit or is_all_caps):
            continue

        forms.append(SurfaceForm(raw, chunk_start + s, chunk_start + e))
        if len(forms) >= max_forms:
            break

    # Deduplicate by (start,end) and by raw text to reduce redundant queries
    # Keep earliest occurrence of the same surface
    seen_span = set()
    seen_text = set()
    out: list[SurfaceForm] = []
    for f in sorted(forms, key=lambda x: (x.start, x.end)):
        k = (f.start, f.end)
        if k in seen_span:
            continue
        if f.text in seen_text:
            # if same string appears multiple times, keep both spans (for highlighting)
            # but to reduce DB queries we can still query once per string later
            pass
        seen_span.add(k)
        out.append(f)

    return out


# -----------------------------------------------------------------------------
# Main backend
# -----------------------------------------------------------------------------
class PgTrgmTextMiner(BaseTextMiner):
    """
    PostgreSQL pg_trgm text miner (token-targeted).
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
            "surface_forms": 0,
            "unique_surface_forms": 0,
            "exact_queries": 0,
            "pg_queries": 0,
            "rows": 0,
            "candidates": 0,
            "spans": 0,
            "fallback_windows": 0,
        }

        try:
            for chunk_start, chunk_end, chunk_text in self._iter_chunks(raw_text, cfg):
                stats["chunks"] += 1

                # 1) Surface-form mining (recommended)
                if self.cfg.prefer_surface_forms:
                    forms = _extract_surface_forms(
                        chunk_text,
                        chunk_start=chunk_start,
                        cfg=cfg,
                        max_forms=self.cfg.max_surface_forms_per_chunk,
                    )
                    stats["surface_forms"] += len(forms)

                    # query once per unique text, then attach candidates to all spans
                    by_text: dict[str, list[SurfaceForm]] = {}
                    for f in forms:
                        by_text.setdefault(f.text, []).append(f)
                    stats["unique_surface_forms"] += len(by_text)

                    # for sf_text, spans_for_text in by_text.items():
                    #     # Try exact first (very fast + best precision)
                    #     exact_rows = self._exact_query(sf_text, cfg=cfg)
                        # stats["exact_queries"] += 1
                        # stats["rows"] += len(exact_rows)

                        # rows_to_use = exact_rows
                        # used_exact = len(exact_rows) > 0

                        # # If no exact, fallback to pg_trgm for this token
                        # if not used_exact or not self.cfg.short_circuit_on_exact:
                        #     if not used_exact:
                        #         trgm_rows = self._pg_trgm_query(sf_text, cfg=cfg)
                        #         stats["pg_queries"] += 1
                        #         stats["rows"] += len(trgm_rows)
                        #         rows_to_use = trgm_rows

                    # ---- NEW: exact recall for all unique surface forms in one query
                    surface_texts = list(by_text.keys())
                    exact_rows = self._exact_surface_query(surface_texts, cfg=cfg)
                    stats["exact_queries"] += 1
                    stats["rows"] += len(exact_rows)

                    # Group rows by normalized match key so we can attach them back to spans
                    # (we match back by lower(alias_value) or alias_norm == surface_lower)
                    rows_by_key: dict[str, list] = {}
                    for r in exact_rows:
                        # r.method could be set by query; if not, infer
                        # Prefer alias_value exact (lower) if it equals the surface; else alias_norm exact.
                        # We'll just store under BOTH keys to simplify attaching.
                        if r.alias_value:
                            rows_by_key.setdefault(str(r.alias_value).lower(), []).append(r)
                        if r.alias_norm:
                            rows_by_key.setdefault(str(r.alias_norm).lower(), []).append(r)

                    # For each surface form, decide rows_to_use:
                    for sf_text, spans_for_text in by_text.items():
                        key = (sf_text or "").strip().lower()
                        used_exact_rows = rows_by_key.get(key, [])
                        rows_to_use = used_exact_rows
                        used_exact = len(used_exact_rows) > 0

                        # If no exact, fallback to pg_trgm for this surface-form
                        if (not used_exact) or (not self.cfg.short_circuit_on_exact):
                            if not used_exact:
                                trgm_rows = self._pg_trgm_query(sf_text, cfg=cfg)
                                stats["pg_queries"] += 1
                                stats["rows"] += len(trgm_rows)
                                rows_to_use = trgm_rows

                        for r in rows_to_use:
                            cand = MentionCandidate(
                                entity_id=int(r.entity_id),
                                group_id=int(r.group_id) if r.group_id is not None else None,
                                entity_type=str(r.group_id) if r.group_id is not None else None,
                                primary_name=r.primary_name,
                                matched_name=r.alias_norm or r.alias_value,
                                matched_name_id=int(r.alias_id) if r.alias_id is not None else None,
                                method=str(getattr(r, "method", "pg_trgm")),
                                # exact query might not have pg_trgm_score; guard it
                                score=float(getattr(r, "pg_trgm_score", 1.0)) * 100.0,
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
                                    "pg_trgm_score": float(getattr(r, "pg_trgm_score", 1.0)),
                                },
                            )
                            stats["candidates"] += 1

                            for f in spans_for_text:
                                sp = Span(start=f.start, end=f.end, text=raw_text[f.start:f.end])
                                stats["spans"] += 1
                                key2 = (sp.start, sp.end)
                                m = mentions_map.get(key2)
                                if m is None:
                                    m = Mention(span=sp, candidates=[], best=None)
                                    mentions_map[key2] = m
                                m.candidates.append(cand)

                # 2) Optional legacy fallback: chunk-window pg_trgm
                # Only use this if you explicitly enable include_chunk_query/min_window_tokens.
                if self.cfg.include_chunk_query:
                    nq = self.normalizer.build(chunk_text)
                    windows = list(_iter_chunk_windows(nq.strict or "", nq.tokens, cfg=self.cfg))
                    stats["fallback_windows"] += len(windows)

                    for qwin in windows:
                        rows = self._pg_trgm_query(qwin, cfg=cfg)
                        stats["pg_queries"] += 1
                        stats["rows"] += len(rows)

                        for r in rows:
                            # Best-effort span: try alias_value/norm in raw text, else chunk span
                            alias_value = r.alias_value
                            alias_norm = r.alias_norm

                            # Prefer alias_value match for actual highlights
                            spans = []
                            if alias_value:
                                for s, e in _find_spans_case_insensitive(raw_text, alias_value):
                                    spans.append(Span(start=s, end=e, text=raw_text[s:e]))
                            if not spans and alias_norm:
                                for s, e in _find_spans_case_insensitive(raw_text, alias_norm):
                                    spans.append(Span(start=s, end=e, text=raw_text[s:e]))
                            if not spans:
                                cs = max(0, chunk_start)
                                ce = min(len(raw_text), chunk_end)
                                if ce > cs:
                                    spans.append(Span(start=cs, end=ce, text=raw_text[cs:ce]))

                            cand = MentionCandidate(
                                entity_id=int(r.entity_id),
                                group_id=int(r.group_id) if r.group_id is not None else None,
                                entity_type=str(r.group_id) if r.group_id is not None else None,
                                primary_name=r.primary_name,
                                matched_name=r.alias_norm or r.alias_value,
                                matched_name_id=int(r.alias_id) if r.alias_id is not None else None,
                                method="pg_trgm",
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

            mentions = list(mentions_map.values())
            mentions = postprocess_mentions(mentions, config=cfg)

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
    def _exact_query(self, surface: str, *, cfg: TextMinerConfig):
        """
        Exact lookup for a short surface form:
        - alias_value equals raw surface (case-sensitive) OR
          lower(alias_value) equals lower(surface) (case-insensitive exact)
        - OR alias_norm equals strict normalization of the surface

        We return pg_trgm_score=1.0 to align with score scaling (0..100).
        """
        PrimaryAlias = aliased(EntityAlias)

        filters = []
        if self.require_active_alias:
            filters.append(EntityAlias.is_active.is_(True))
        if self.require_active_entity:
            filters.append(Entity.is_active.is_(True))
        if cfg.locale:
            filters.append(EntityAlias.locale == cfg.locale)
        if cfg.data_source_ids:
            filters.append(EntityAlias.data_source_id.in_(cfg.data_source_ids))

        # Group filter (numeric ids only for now)
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

        nq = self.normalizer.build(surface)
        strict = nq.strict or ""
        basic = nq.basic or ""
        raw = nq.raw or surface

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
                func.cast(1.0, func.float).label("pg_trgm_score"),
                func.cast("exact", func.text).label("method"),
            )
            .select_from(EntityAlias)
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .outerjoin(
                PrimaryAlias,
                (PrimaryAlias.entity_id == Entity.id) & (PrimaryAlias.is_primary.is_(True)),
            )
            .where(*filters)
            .where(
                or_(
                    EntityAlias.alias_value.in_([raw, basic]),
                    func.lower(EntityAlias.alias_value) == func.lower(surface),
                    EntityAlias.alias_norm == strict,
                )
            )
            .order_by(EntityAlias.is_primary.desc(), EntityAlias.id.asc())
            .limit(50)
        )

        rows = self.session.execute(stmt).all()

        # Patch method label so candidates can report exact_name vs exact_normalized
        # without changing SQL.
        out = []
        for r in rows:
            # Decide method from which field matched (best-effort)
            method = "exact_name"
            if r.alias_norm and strict and (r.alias_norm == strict):
                method = "exact_normalized"
            elif r.alias_value and (r.alias_value == raw or r.alias_value == basic):
                method = "exact_name"
            else:
                # fallback: treat as exact_name
                method = "exact_name"

            # Create a lightweight row-like object by copying to dict-like namespace
            # so downstream logic can read r.method.
            r_dict = dict(r._mapping)
            r_dict["method"] = method
            out.append(type("Row", (), r_dict))

        return out

    # ---------------------------------------------------------------------
    def _pg_trgm_query(self, query_text: str, *, cfg: TextMinerConfig):
        PrimaryAlias = aliased(EntityAlias)

        filters = []
        if self.require_active_alias:
            filters.append(EntityAlias.is_active.is_(True))
        if self.require_active_entity:
            filters.append(Entity.is_active.is_(True))
        if cfg.locale:
            filters.append(EntityAlias.locale == cfg.locale)
        if cfg.data_source_ids:
            filters.append(EntityAlias.data_source_id.in_(cfg.data_source_ids))

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

        sim_norm = func.similarity(EntityAlias.alias_norm, query_text)
        trigram_match_norm = EntityAlias.alias_norm.op("%")(query_text)

        where_clause = or_(
            trigram_match_norm,
            sim_norm >= self.cfg.similarity_threshold,
        )

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

        rows = self.session.execute(stmt).all()

        # tag method in returned rows (like in exact)
        out = []
        for r in rows:
            r_dict = dict(r._mapping)
            r_dict["method"] = "pg_trgm"
            out.append(type("Row", (), r_dict))
        return out

    # ---------------------------------------------------------------------
    def _exact_surface_query(self, surface_forms: list[str], *, cfg: TextMinerConfig):
        PrimaryAlias = aliased(EntityAlias)

        filters = []
        if self.require_active_alias:
            filters.append(EntityAlias.is_active.is_(True))
        if self.require_active_entity:
            filters.append(Entity.is_active.is_(True))
        if cfg.locale:
            filters.append(EntityAlias.locale == cfg.locale)
        if cfg.data_source_ids:
            filters.append(EntityAlias.data_source_id.in_(cfg.data_source_ids))

        # group filter (numeric ids only, igual ao seu código atual)
        group_ids: list[int] = []
        if cfg.entity_type_hints:
            for h in cfg.entity_type_hints:
                if h and str(h).strip().isdigit():
                    group_ids.append(int(str(h).strip()))
        if group_ids:
            filters.append(EntityAlias.group_id.in_(group_ids))

        # --- IMPORTANT: avoid tuple_.in_ entirely ---
        # Build OR conditions for exact hits.
        # We compare:
        #   lower(alias_value) == lower(surface)
        #   OR alias_norm == strict-like surface (already lower-ish)
        exact_conds = []
        for s in surface_forms:
            s = (s or "").strip()
            if not s:
                continue
            s_lower = s.lower()

            exact_conds.append(func.lower(EntityAlias.alias_value) == s_lower)
            exact_conds.append(EntityAlias.alias_norm == s_lower)

        if not exact_conds:
            return []

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
            )
            .select_from(EntityAlias)
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .outerjoin(
                PrimaryAlias,
                (PrimaryAlias.entity_id == Entity.id) & (PrimaryAlias.is_primary.is_(True)),
            )
            .where(*filters)
            .where(or_(*exact_conds))
            .order_by(EntityAlias.is_primary.desc(), EntityAlias.id.asc())
            .limit(200)
        )

        return self.session.execute(stmt).all()
