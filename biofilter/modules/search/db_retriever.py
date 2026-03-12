from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select, or_, func
from sqlalchemy.orm import Session, aliased

from biofilter.modules.db.models.model_entities import Entity, EntityAlias, EntityGroup
from biofilter.modules.search.types import Candidate, NormalizedQuery

# New: Postgres-only pg_trgm retriever
from biofilter.modules.search.db_retriever_pgtrgm import (  # noqa: E402
    PGTrgmConfig,
    fetch_pgtrgm_candidates,
)


@dataclass(frozen=True)
class DBRetrieverConfig:
    """
    DB-side candidate retrieval settings.

    Notes:
    - `pool_limit` is the maximum size returned to the resolver (pre-fuzzy).
    - `exact_limit` is the cap for strict exact matches (usually small).
    """

    pool_limit: int = 1000
    exact_limit: int = 200
    pool_token_limit: int = 3

    require_active_alias: bool = True
    require_active_entity: bool = True

    locale: Optional[str] = None
    data_source_ids: Optional[list[int]] = None

    prefer_primary_first: bool = True

    # New: pg_trgm layer (Postgres only)
    pgtrgm_enabled: bool = True
    pgtrgm_min_score: float = (
        0.30  # 0..1 in Postgres, we convert to 0..100 in Candidate
    )
    pgtrgm_stop_score: float = 0.99  # stop early if top candidate >= this (0..1)
    pgtrgm_limit: int = 500  # max rows from pg_trgm query


def build_group_name_to_id(session: Session) -> dict[str, int]:
    rows = session.execute(select(EntityGroup.id, EntityGroup.name)).all()
    return {name.lower(): int(gid) for gid, name in rows}


def make_entity_alias_retriever(
    session: Session,
    *,
    cfg: DBRetrieverConfig | None = None,
):
    """
    Returns a retriever(query, pool_limit, entity_type_hints) -> list[Candidate].

    entity_type_hints:
      - list of EntityGroup.name (e.g., ["Chemicals"]) or ids as strings (e.g., ["10"])
    """
    cfg = cfg or DBRetrieverConfig()
    group_map = build_group_name_to_id(session)

    PrimaryAlias = aliased(EntityAlias)

    def _resolve_group_ids(hints: Optional[list[str]]) -> Optional[list[int]]:
        if not hints:
            return None
        gids: list[int] = []
        for h in hints:
            if not h:
                continue
            key = h.strip().lower()
            if key.isdigit():
                gids.append(int(key))
            elif key in group_map:
                gids.append(group_map[key])
        return gids or None

    def _base_filters(group_ids: Optional[list[int]]):
        clauses = []
        if cfg.require_active_alias:
            clauses.append(EntityAlias.is_active.is_(True))
        if cfg.require_active_entity:
            clauses.append(Entity.is_active.is_(True))

        if cfg.locale:
            clauses.append(EntityAlias.locale == cfg.locale)
        if cfg.data_source_ids:
            clauses.append(EntityAlias.data_source_id.in_(cfg.data_source_ids))
        if group_ids:
            clauses.append(EntityAlias.group_id.in_(group_ids))

        return clauses

    def _select_columns():
        # We also fetch the entity primary alias (if present) for display.
        return [
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
        ]

    def retriever(
        query: NormalizedQuery, pool_limit: int, entity_type_hints: Optional[list[str]]
    ):
        """
        Retrieval strategy (in order):

        0) Postgres-only pg_trgm similarity on alias_norm (optional)
           - If top score >= stop_score, short-circuit and return.
           - Otherwise, merge with remaining layers (dedup by (entity_id, alias_id)).

        1) Exact: alias_value in {raw, basic} OR alias_norm == strict
        2) Pool: prefix-based recall using tokens -> used by fuzzy scorer downstream
        """
        group_ids = _resolve_group_ids(entity_type_hints)
        base_filters = _base_filters(group_ids)

        candidates: list[Candidate] = []
        seen = set()  # (entity_id, alias_id)

        # ---------------------------------------------------------------------
        # 0) Postgres-only pg_trgm layer
        # ---------------------------------------------------------------------
        if cfg.pgtrgm_enabled:
            pg_cfg = PGTrgmConfig(
                enabled=True,
                min_score=cfg.pgtrgm_min_score,
                stop_score=cfg.pgtrgm_stop_score,
                limit=min(cfg.pgtrgm_limit, max(50, pool_limit)),
                require_active_alias=cfg.require_active_alias,
                require_active_entity=cfg.require_active_entity,
            )

            pg_candidates, stop_early = fetch_pgtrgm_candidates(
                session,
                query,
                entity_type_hints=entity_type_hints,
                group_map=group_map,
                cfg=pg_cfg,
                locale=cfg.locale,
            )

            for c in pg_candidates:
                key = (int(c.entity_id), int(c.matched_name_id))
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(c)

            # If we got a near-perfect match, treat it as "exact enough"
            if stop_early and candidates:
                return candidates

        # ---------------------------------------------------------------------
        # 1) Exact statement
        # ---------------------------------------------------------------------
        exact_value_terms = list({query.raw, query.basic})
        exact_norm_term = query.strict

        exact_stmt = (
            select(*_select_columns())
            .select_from(EntityAlias)
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .outerjoin(
                PrimaryAlias,
                (PrimaryAlias.entity_id == Entity.id)
                & (PrimaryAlias.is_primary.is_(True)),
            )
            .where(*base_filters)
            .where(
                or_(
                    EntityAlias.alias_value.in_(exact_value_terms),
                    EntityAlias.alias_norm == exact_norm_term,
                )
            )
        )

        if cfg.prefer_primary_first:
            exact_stmt = exact_stmt.order_by(
                EntityAlias.is_primary.desc(),
                EntityAlias.id.asc(),
            )

        exact_rows = session.execute(exact_stmt.limit(cfg.exact_limit)).all()

        for r in exact_rows:
            if r.alias_value in exact_value_terms:
                matched = r.alias_value
                method = "exact_name"
            elif r.alias_norm == exact_norm_term:
                matched = r.alias_norm
                method = "exact_normalized"
            else:
                matched = r.alias_norm or r.alias_value
                method = "db_pool"

            key = (int(r.entity_id), int(r.alias_id))
            if key in seen:
                continue
            seen.add(key)

            candidates.append(
                Candidate(
                    entity_id=int(r.entity_id),
                    entity_type=str(r.group_id) if r.group_id is not None else None,
                    primary_name=r.primary_name,
                    matched_name=matched,
                    matched_name_id=int(r.alias_id),
                    method=method,
                    score=0.0,
                    data_source=(
                        str(r.data_source_id) if r.data_source_id is not None else None
                    ),
                    meta={
                        "group_id": int(r.group_id) if r.group_id is not None else None,
                        "alias_type": r.alias_type,
                        "xref_source": r.xref_source,
                        "locale": r.locale,
                        "is_primary_name": bool(r.is_primary),
                        "etl_package_id": r.etl_package_id,
                        "alias_value": r.alias_value,
                        "alias_norm": r.alias_norm,
                    },
                )
            )

        # ---------------------------------------------------------------------
        # 2) Pool statement (prefix recall)
        # ---------------------------------------------------------------------
        tokens = list(query.tokens)[: cfg.pool_token_limit]
        if not tokens and query.strict:
            tokens = [query.strict.split(" ")[0]]

        like_conds = []
        for tok in tokens:
            if tok and len(tok) >= 2:
                like_conds.append(EntityAlias.alias_norm.like(f"{tok}%"))

        if not like_conds and query.basic:
            like_conds.append(
                func.lower(EntityAlias.alias_value).like(f"{query.basic[:3]}%")
            )

        if like_conds:
            pool_stmt = (
                select(*_select_columns())
                .select_from(EntityAlias)
                .join(Entity, Entity.id == EntityAlias.entity_id)
                .outerjoin(
                    PrimaryAlias,
                    (PrimaryAlias.entity_id == Entity.id)
                    & (PrimaryAlias.is_primary.is_(True)),
                )
                .where(*base_filters)
                .where(or_(*like_conds))
            )

            if cfg.prefer_primary_first:
                pool_stmt = pool_stmt.order_by(
                    EntityAlias.is_primary.desc(),
                    EntityAlias.id.asc(),
                )

            pool_rows = session.execute(
                pool_stmt.limit(min(pool_limit, cfg.pool_limit))
            ).all()

            for r in pool_rows:
                key = (int(r.entity_id), int(r.alias_id))
                if key in seen:
                    continue
                seen.add(key)

                matched = r.alias_norm or r.alias_value

                candidates.append(
                    Candidate(
                        entity_id=int(r.entity_id),
                        entity_type=str(r.group_id) if r.group_id is not None else None,
                        primary_name=r.primary_name,
                        matched_name=matched,
                        matched_name_id=int(r.alias_id),
                        method="db_pool",
                        score=0.0,
                        data_source=(
                            str(r.data_source_id)
                            if r.data_source_id is not None
                            else None
                        ),
                        meta={
                            "group_id": (
                                int(r.group_id) if r.group_id is not None else None
                            ),
                            "alias_type": r.alias_type,
                            "xref_source": r.xref_source,
                            "locale": r.locale,
                            "is_primary_name": bool(r.is_primary),
                            "etl_package_id": r.etl_package_id,
                            "alias_value": r.alias_value,
                            "alias_norm": r.alias_norm,
                        },
                    )
                )

        return candidates

    return retriever
