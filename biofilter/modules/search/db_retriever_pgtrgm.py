# biofilter/modules/search/db_retriever_pgtrgm.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from sqlalchemy import BigInteger, Integer, String, and_, func, select
from sqlalchemy.orm import Session

from biofilter.modules.search.types import Candidate, NormalizedQuery
from biofilter.modules.db.models.model_entities import Entity, EntityAlias  # adjust import path if needed


@dataclass(frozen=True)
class PGTrgmConfig:
    """
    Postgres pg_trgm candidate retrieval config.

    Notes:
    - Requires `CREATE EXTENSION pg_trgm;`
    - Strongly recommended: GIN index on entity_aliases.alias_norm using gin_trgm_ops
      e.g. CREATE INDEX ... USING gin (alias_norm gin_trgm_ops);

    Typical settings:
    - min_score ~ 0.20-0.35 (recall vs precision)
    - stop_score ~ 0.99 (treat as exact)
    - limit ~ 200-1000 depending on DB size
    """

    enabled: bool = True
    min_score: float = 0.30
    stop_score: float = 0.99
    limit: int = 500

    # Safety knobs
    require_active_alias: bool = True
    require_active_entity: bool = True


def is_postgres(session: Session) -> bool:
    """
    Return True if the session is bound to a PostgreSQL engine.
    """
    bind = session.get_bind()
    return bool(bind) and bind.dialect.name == "postgresql"


def _coerce_group_ids(entity_type_hints: Sequence[str] | None, group_map: dict[str, int] | None) -> list[int]:
    """
    Convert entity_type_hints (names like 'Chemicals', 'Genes') to group_ids when possible.
    If hints are already numeric strings, accept them as ids.

    If no hints provided, returns empty list (caller may decide to not filter by group_id).
    """
    if not entity_type_hints:
        return []

    out: list[int] = []
    for h in entity_type_hints:
        if h is None:
            continue
        s = str(h).strip()
        if not s:
            continue
        if s.isdigit():
            out.append(int(s))
            continue
        if group_map and s in group_map:
            out.append(int(group_map[s]))
    # de-dup while preserving order
    seen = set()
    dedup: list[int] = []
    for gid in out:
        if gid in seen:
            continue
        seen.add(gid)
        dedup.append(gid)
    return dedup


def fetch_pgtrgm_candidates(
    session: Session,
    query: NormalizedQuery,
    *,
    entity_type_hints: Sequence[str] | None = None,
    group_map: dict[str, int] | None = None,
    cfg: PGTrgmConfig | None = None,
    locale: str | None = None,
) -> tuple[list[Candidate], bool]:
    """
    Retrieve candidates using pg_trgm similarity against EntityAlias.alias_norm.

    Returns:
        (candidates, stop_early)

    stop_early is True when the top candidate score >= cfg.stop_score.

    Important:
    - This function is Postgres-only. Caller should check is_postgres(session).
    - It assumes query.strict is the best normalized string for comparison.
      If query.strict is empty, it will fallback to query.basic, then raw.

    Implementation details:
    - Uses Postgres function similarity(alias_norm, :q)
    - Filters by similarity >= cfg.min_score
    - Optional group_id filter based on entity_type_hints
    - Optional locale filter
    - Optional active flags on alias/entity
    """

    cfg = cfg or PGTrgmConfig()

    if not cfg.enabled:
        return ([], False)

    if not is_postgres(session):
        return ([], False)

    q = (query.strict or query.basic or query.raw or "").strip()
    if not q:
        return ([], False)

    group_ids = _coerce_group_ids(entity_type_hints, group_map)

    # ---- Base query
    score_expr = func.similarity(EntityAlias.alias_norm, q).label("score")

    stmt = (
        select(
            EntityAlias.id.label("alias_id"),
            EntityAlias.entity_id.label("entity_id"),
            EntityAlias.group_id.label("group_id"),
            EntityAlias.alias_value.label("alias_value"),
            EntityAlias.alias_norm.label("alias_norm"),
            EntityAlias.alias_type.label("alias_type"),
            EntityAlias.xref_source.label("xref_source"),
            EntityAlias.locale.label("locale"),
            EntityAlias.is_primary.label("is_primary"),
            EntityAlias.data_source_id.label("data_source_id"),
            EntityAlias.etl_package_id.label("etl_package_id"),
            score_expr,
        )
        .select_from(EntityAlias)
        .join(Entity, Entity.id == EntityAlias.entity_id)
        .where(EntityAlias.alias_norm.isnot(None))
        .where(score_expr >= float(cfg.min_score))
    )

    if group_ids:
        stmt = stmt.where(EntityAlias.group_id.in_(group_ids))

    if locale:
        stmt = stmt.where(EntityAlias.locale == locale)

    if cfg.require_active_alias:
        # In your schema, is_active can be NULL. Treat NULL as active unless you want strict True.
        stmt = stmt.where(EntityAlias.is_active.is_(True))

    if cfg.require_active_entity:
        stmt = stmt.where(Entity.is_active.is_(True))

    stmt = stmt.order_by(score_expr.desc()).limit(int(cfg.limit))

    rows = session.execute(stmt).all()
    if not rows:
        return ([], False)

    candidates: list[Candidate] = []
    seen = set()  # (entity_id, alias_id)

    for r in rows:
        key = (int(r.entity_id), int(r.alias_id))
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            Candidate(
                entity_id=int(r.entity_id),
                entity_type=str(r.group_id) if r.group_id is not None else None,
                primary_name=None,  # filled later by resolver or other retrievers
                matched_name=(r.alias_norm or r.alias_value),
                matched_name_id=int(r.alias_id),
                method="pg_trgm",
                score=float(r.score) * 100.0,  # normalize to 0..100 scale to match your resolver
                data_source=str(r.data_source_id) if r.data_source_id is not None else None,
                meta={
                    "group_id": int(r.group_id) if r.group_id is not None else None,
                    "alias_type": r.alias_type,
                    "xref_source": r.xref_source,
                    "locale": r.locale,
                    "is_primary_name": bool(r.is_primary),
                    "etl_package_id": r.etl_package_id,
                    # Mirror full alias fields for consumers
                    "alias_value": r.alias_value,
                    "alias_norm": r.alias_norm,
                    # Keep raw pg_trgm score too (0..1) for debugging
                    "pg_trgm_score": float(r.score),
                },
            )
        )

    top = candidates[0]
    stop_early = (top.meta.get("pg_trgm_score", 0.0) >= float(cfg.stop_score))

    return (candidates, stop_early)
