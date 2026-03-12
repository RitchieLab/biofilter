from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import or_, select
from sqlalchemy.orm import aliased

from biofilter.modules.report.reports.base_report import ReportBase

# IMPORTANT:
# Prefer importing from the same place your other code uses.
# If you already re-export these in biofilter.db.models, you can switch to:
#   from biofilter.db.models import Entity, EntityAlias, EntityGroup, EntityRelationship
from biofilter.modules.db.models.model_entities import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
)

# Search / Resolver
from biofilter.modules.search.resolver import TermResolver, ResolverConfig
from biofilter.modules.search.db_retriever import (
    make_entity_alias_retriever,
    DBRetrieverConfig,
)

try:
    # This matches the NormalizedQuery object you showed in the resolver output
    from biofilter.modules.search.normalizers import normalize_basic  # type: ignore
except Exception:  # pragma: no cover
    normalize_basic = None  # fallback


@dataclass(frozen=True)
class InputItem:
    type_hint: str | None
    word: str


class EntityNeighborhoodSummaryReport(ReportBase):
    """
    Entity Neighborhood Summary Report (v1)

    - Resolve (type_hint, word) inputs to Entities (exact alias_norm or optional search/hybrid)
    - Return a 1-hop neighborhood summarized by neighbor domain/group
    - Neighbor columns are JSON strings (e.g. '["BRCA1","TP53"]')
    """

    name = "entity_neighborhood_summary"
    description = (
        "Resolve inputs to entities (exact alias_norm or optional search resolver) "
        "and return 1-hop neighborhood summarized by neighbor domain types."
    )

    columns = [
        # Standard
        "Input Word",
        "Input Type Hint",
        "Resolver Mode",
        "Entity ID",
        "Entity Type",
        "Canonical Name",
        "Primary Alias",
        "Aliases Top",
        "Alias Count",
        "Degree Total (1-hop)",
        "Degree By Type (1-hop)",
        # Resolver
        "Resolve Status",
        "Resolve Method",
        "Resolve Score",
        "Resolve Meta",
        "Resolve Candidates",
        # Dynamic neighbors
        "Genes",
        "Proteins",
        "Pathways",
        "GO Terms",
        "Diseases",
        "Chemicals",
    ]

    # Internal cache for resolver
    _resolver: Optional[TermResolver] = None

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def explain(cls) -> str:
        return """\
🧭 Entity Neighborhood Summary Report (v1)
=========================================

Inputs:
- items: list[str] like ["gene:BRCA1", "disease:Alzheimer disease", "APOE"]
- input: TSV/CSV file (type_hint<TAB>word preferred)

Resolution modes:
- resolver_mode="exact" (default):
  - EntityAlias.alias_norm exact match (normalized)
  - if multiple matches exist, emits ONE output row PER match (Policy A)
- resolver_mode="search":
  - TermResolver.resolve_best(...) (pg_trgm supported on Postgres)
  - emits ONE row per input (best candidate), status preserved (resolved/ambiguous/not_found)
- resolver_mode="hybrid":
  - exact first:
      - if exactly 1 match => keep exact
      - if 0 or >1 => fallback to resolve_best => emits ONE row (best) for that input

Output:
- Neighbor columns are JSON strings.
- Degree is computed after truncation (bounded output).

Key params:
- resolver_mode: exact|search|hybrid
- limit_neighbors (default 200)
- limit_neighbors_per_type (default 50)
- include_all_aliases (default False)
- aliases_top_n (default 20)
- include_candidates (default False)
- candidates_top_n (default 10)
- emit_not_found_rows (default False)
"""

    @classmethod
    def example_input(cls) -> dict[str, Any]:
        return {
            "items": [
                "gene:BRCA1",
                "disease:Alzheimer disease",
                "chemical:((R)-3-Hydroxybutanoyl)(n-2)",
                "APOE",
            ],
            "resolver_mode": "hybrid",
            "limit_neighbors": 200,
            "limit_neighbors_per_type": 50,
            "include_candidates": True,
            "candidates_top_n": 10,
        }

    # ---------------------------------------------------------------------
    # Helpers: input parsing
    # ---------------------------------------------------------------------
    def _parse_item_arg(self, raw: str) -> InputItem:
        raw = (raw or "").strip()
        if not raw:
            raise ValueError("Empty item")

        if ":" not in raw:
            return InputItem(type_hint=None, word=raw)

        left, right = raw.split(":", 1)
        left = left.strip() or None
        right = right.strip()
        if not right:
            raise ValueError(f"Invalid item (missing word): {raw}")

        return InputItem(type_hint=left, word=right)

    def _load_items_from_file(self, path_in: str) -> list[InputItem]:
        p = Path(path_in)
        if not p.exists():
            raise FileNotFoundError(f"Input file not found: {path_in}")

        suffix = p.suffix.lower()

        # TSV / TXT (preferred)
        if suffix in (".tsv", ".txt"):
            rows: list[InputItem] = []
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line.strip() or line.lstrip().startswith("#"):
                        continue
                    parts = line.split("\t")

                    # optional header
                    if parts and parts[0].strip().lower() == "type_hint":
                        continue

                    if len(parts) == 1:
                        wd = parts[0].strip()
                        if wd:
                            rows.append(InputItem(None, wd))
                    else:
                        th = parts[0].strip() or None
                        wd = parts[1].strip()
                        if wd:
                            rows.append(InputItem(th, wd))
            return rows

        # CSV
        df = pd.read_csv(p)
        cols = {c.strip().lower(): c for c in df.columns}

        # header case
        if "word" in cols:
            word_col = cols["word"]
            type_col = cols.get("type_hint")
            items: list[InputItem] = []
            for _, r in df.iterrows():
                wd = str(r[word_col]).strip() if pd.notna(r[word_col]) else ""
                if not wd:
                    continue
                th = None
                if type_col and pd.notna(r[type_col]):
                    th = str(r[type_col]).strip() or None
                items.append(InputItem(th, wd))
            return items

        # no header case
        if df.shape[1] == 1:
            return [
                InputItem(None, str(x).strip())
                for x in df.iloc[:, 0].tolist()
                if str(x).strip()
            ]

        if df.shape[1] >= 2:
            out: list[InputItem] = []
            for th, wd in zip(df.iloc[:, 0].tolist(), df.iloc[:, 1].tolist()):
                wd = str(wd).strip()
                if not wd:
                    continue
                th = str(th).strip() or None
                out.append(InputItem(th, wd))
            return out

        return []

    def _collect_inputs(self) -> list[InputItem]:
        items: list[InputItem] = []

        path_in = self.params.get("input")
        if path_in:
            items.extend(self._load_items_from_file(str(path_in)))

        raw_items = self.params.get("items") or []
        if isinstance(raw_items, str):
            raw_items = [raw_items]

        for x in raw_items:
            if isinstance(x, (list, tuple)) and len(x) == 2:
                th = x[0] or None
                wd = str(x[1]).strip()
                if wd:
                    items.append(InputItem(th, wd))
            else:
                items.append(self._parse_item_arg(str(x)))

        return [it for it in items if it.word and it.word.strip()]

    # ---------------------------------------------------------------------
    # Helpers: normalization & type hints
    # ---------------------------------------------------------------------
    def _norm(self, s: str) -> str:
        """
        Normalize to match EntityAlias.alias_norm.

        Prefer using the same normalizer as the Search module, since your
        NormalizedQuery.basic appears to match alias_norm usage.
        """
        s = (s or "").strip()
        if not s:
            return s
        if normalize_basic is not None:
            try:
                nq = normalize_basic(s)
                # Typical structure: NormalizedQuery(raw, basic, strict, tokens)
                return str(getattr(nq, "basic", s)).strip()
            except Exception:
                return s.lower()
        return s.lower()

    def _normalize_type_hint(self, th: str | None) -> str | None:
        if th is None:
            return None
        th2 = str(th).strip()
        return th2 or None

    def _hint_to_group_name(self, th: str | None) -> list[str]:
        """
        Convert user type hints into resolver entity_type_hints.
        Your integration tests use entity_type_hints=["Chemicals"], i.e. DB group names.

        - If th is None => []
        - If th looks like "Chemicals" already => ["Chemicals"]
        - If th is "chemical" => ["Chemicals"] (mapped)
        """
        if not th:
            return []

        raw = str(th).strip()
        low = raw.lower()

        # Common mappings (adjust if your EntityGroup names differ)
        mapping = {
            "gene": "Genes",
            "genes": "Genes",
            "disease": "Diseases",
            "diseases": "Diseases",
            "chemical": "Chemicals",
            "chemicals": "Chemicals",
            "pathway": "Pathways",
            "pathways": "Pathways",
            "protein": "Proteins",
            "proteins": "Proteins",
            "go": "GO Terms",
            "go_terms": "GO Terms",
            "goterms": "GO Terms",
        }

        if low in mapping:
            return [mapping[low]]

        # Already in DB-style capitalization? keep it
        return [raw]

    # ---------------------------------------------------------------------
    # Resolver builder (search)
    # ---------------------------------------------------------------------
    def _get_resolver(self) -> TermResolver:
        """
        Build (and cache) a TermResolver that matches your integration conftest.

        You can override thresholds via report params if desired.
        """
        if self._resolver is not None:
            return self._resolver

        session = self.session

        # Retriever config (defaults based on your conftest.py)
        retriever_cfg = DBRetrieverConfig(
            locale=str(self.params.get("search_locale", "en")),
            pool_limit=int(self.params.get("search_pool_limit", 1500)),
            exact_limit=int(self.params.get("search_exact_limit", 300)),
            prefer_primary_first=bool(self.params.get("prefer_primary_first", True)),
            require_active_alias=bool(self.params.get("require_active_alias", True)),
            require_active_entity=bool(self.params.get("require_active_entity", True)),
            # pg_trgm layer
            pgtrgm_enabled=bool(self.params.get("pgtrgm_enabled", True)),
            pgtrgm_min_score=float(self.params.get("pgtrgm_min_score", 0.30)),
            pgtrgm_stop_score=float(self.params.get("pgtrgm_stop_score", 0.99)),
            pgtrgm_limit=int(self.params.get("pgtrgm_limit", 500)),
        )

        retriever = make_entity_alias_retriever(session, cfg=retriever_cfg)

        resolver_cfg = ResolverConfig(
            pool_limit=int(self.params.get("resolver_pool_limit", 1500)),
            top_k=int(self.params.get("resolver_top_k", 20)),
            min_score=float(self.params.get("resolver_min_score", 90.0)),
            min_delta=float(self.params.get("resolver_min_delta", 5.0)),
            enable_fuzzy_fallback=bool(self.params.get("enable_fuzzy_fallback", True)),
        )

        self._resolver = TermResolver(retriever, config=resolver_cfg)
        return self._resolver

    # ---------------------------------------------------------------------
    # Helpers: group name mapping for output
    # ---------------------------------------------------------------------
    def _groupname_to_entity_type(self, group_name: str) -> str:
        g = (group_name or "").strip().lower()
        if g == "genes":
            return "gene"
        if g == "diseases":
            return "disease"
        if g == "chemicals":
            return "chemical"
        if g == "pathways":
            return "pathway"
        if g in ("go terms", "goterms"):
            return "go_term"
        if g == "proteins":
            return "protein"
        return g or "unknown"

    def _groupname_to_neighbor_col(self, group_name: str) -> str | None:
        g = (group_name or "").strip().lower()
        if g == "genes":
            return "Genes"
        if g == "proteins":
            return "Proteins"
        if g == "pathways":
            return "Pathways"
        if g in ("go terms", "goterms"):
            return "GO Terms"
        if g == "diseases":
            return "Diseases"
        if g == "chemicals":
            return "Chemicals"
        return None

    # ---------------------------------------------------------------------
    # Core
    # ---------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        resolver_mode = str(self.params.get("resolver_mode", "exact")).strip().lower()
        if resolver_mode not in ("exact", "search", "hybrid"):
            self.logger.log(
                "resolver_mode must be exact|search|hybrid. Using exact.", "WARNING"
            )
            resolver_mode = "exact"

        limit_neighbors = int(self.params.get("limit_neighbors", 200))
        limit_neighbors_per_type = int(self.params.get("limit_neighbors_per_type", 50))

        include_all_aliases = bool(self.params.get("include_all_aliases", False))
        aliases_top_n = int(self.params.get("aliases_top_n", 20))

        include_candidates = bool(self.params.get("include_candidates", False))
        candidates_top_n = int(self.params.get("candidates_top_n", 10))

        emit_not_found_rows = bool(self.params.get("emit_not_found_rows", False))

        items = self._collect_inputs()
        if not items:
            self.logger.log("No inputs provided (items or input file).", "ERROR")
            return pd.DataFrame()

        bind = self.session.get_bind()

        # Initialize neighbor columns
        neighbor_cols = [
            "Genes",
            "Proteins",
            "Pathways",
            "GO Terms",
            "Diseases",
            "Chemicals",
        ]

        # -----------------------------------------------------------------
        # Step 1) Resolve entities
        # -----------------------------------------------------------------
        resolved_rows: list[dict[str, Any]] = []

        # ---------- EXACT ----------
        if resolver_mode == "exact":
            # Batch query: alias_norm in norms, filter by group if type_hint provided
            # Policy A: multiple rows per match
            PrimaryAlias = aliased(EntityAlias)

            # We bucket by normalized type_hint for filtering
            buckets: dict[str | None, list[InputItem]] = {}
            for it in items:
                th = self._normalize_type_hint(it.type_hint)
                buckets.setdefault(th, []).append(it)

            # Preload group ids by name for filtering
            group_name_to_id = {
                g.name: g.id for g in self.session.query(EntityGroup).all()
            }
            # mapping type_hint -> EntityGroup.name
            hint_to_group_name = {
                "gene": "Genes",
                "genes": "Genes",
                "disease": "Diseases",
                "diseases": "Diseases",
                "chemical": "Chemicals",
                "chemicals": "Chemicals",
                "pathway": "Pathways",
                "pathways": "Pathways",
                "protein": "Proteins",
                "proteins": "Proteins",
                "go": "GO Terms",
                "go_terms": "GO Terms",
                "goterms": "GO Terms",
            }

            for th, bucket in buckets.items():
                norms = [self._norm(b.word) for b in bucket]
                norm_to_word = {self._norm(b.word): b.word for b in bucket}

                q = (
                    self.session.query(
                        EntityAlias.alias_norm.label("alias_norm"),
                        Entity.id.label("entity_id"),
                        Entity.group_id.label("group_id"),
                        PrimaryAlias.alias_value.label("primary_alias"),
                    )
                    .join(Entity, Entity.id == EntityAlias.entity_id)
                    .join(
                        PrimaryAlias,
                        (PrimaryAlias.entity_id == Entity.id)
                        & (PrimaryAlias.is_primary.is_(True)),
                    )
                    .filter(EntityAlias.alias_norm.in_(norms))
                )

                # optional group filter
                if th:
                    gname = hint_to_group_name.get(th.lower())
                    if gname and gname in group_name_to_id:
                        q = q.filter(Entity.group_id == group_name_to_id[gname])

                with bind.connect() as conn:
                    df = pd.read_sql(q.statement, conn)

                if df.empty:
                    if emit_not_found_rows:
                        for b in bucket:
                            row = self._make_empty_row(
                                resolver_mode, b.word, b.type_hint
                            )
                            row["Resolve Status"] = "not_found"
                            resolved_rows.append(row)
                    continue

                for _, r in df.iterrows():
                    input_word = norm_to_word.get(
                        str(r["alias_norm"]), str(r["alias_norm"])
                    )
                    row = self._make_base_row(
                        resolver_mode=resolver_mode,
                        input_word=input_word,
                        input_type_hint=th,
                        entity_id=int(r["entity_id"]),
                        primary_alias=r.get("primary_alias"),
                        resolve_status="resolved",
                        resolve_method="exact",
                        resolve_score=1.0,
                        resolve_meta=None,
                        resolve_candidates=None,
                    )
                    resolved_rows.append(row)

        # ---------- SEARCH ----------
        elif resolver_mode == "search":
            resolver = self._get_resolver()

            for it in items:
                hints = self._hint_to_group_name(it.type_hint)
                res = resolver.resolve_best(it.word, entity_type_hints=hints)

                if res.best is None:
                    if emit_not_found_rows:
                        row = self._make_empty_row(resolver_mode, it.word, it.type_hint)
                        row["Resolve Status"] = getattr(res, "status", "not_found")
                        resolved_rows.append(row)
                    continue

                best = res.best
                meta = getattr(best, "meta", {}) or {}

                row = self._make_base_row(
                    resolver_mode=resolver_mode,
                    input_word=it.word,
                    input_type_hint=it.type_hint,
                    entity_id=int(best.entity_id),
                    primary_alias=None,  # filled later from DB
                    resolve_status=getattr(res, "status", None),
                    resolve_method=getattr(best, "method", None),
                    resolve_score=(
                        float(getattr(best, "score", 0.0))
                        if getattr(best, "score", None) is not None
                        else None
                    ),
                    resolve_meta=json.dumps(meta) if meta else None,
                    resolve_candidates=(
                        self._serialize_candidates(res, candidates_top_n)
                        if include_candidates
                        else None
                    ),
                )

                resolved_rows.append(row)

        # ---------- HYBRID ----------
        else:  # hybrid
            resolver = self._get_resolver()

            # First do exact matching, but keep per-input match lists
            # Key = (type_hint_norm, alias_norm)
            type_hint_norm = lambda x: (str(x).strip().lower() if x else None)
            key_for = lambda th, word: (type_hint_norm(th), self._norm(word))

            # Bucketed exact query (per type_hint) to reduce DB calls
            buckets: dict[str | None, list[InputItem]] = {}
            for it in items:
                buckets.setdefault(type_hint_norm(it.type_hint), []).append(it)

            # group filter prep
            group_name_to_id = {
                g.name: g.id for g in self.session.query(EntityGroup).all()
            }
            hint_to_group_name = {
                "gene": "Genes",
                "genes": "Genes",
                "disease": "Diseases",
                "diseases": "Diseases",
                "chemical": "Chemicals",
                "chemicals": "Chemicals",
                "pathway": "Pathways",
                "pathways": "Pathways",
                "protein": "Proteins",
                "proteins": "Proteins",
                "go": "GO Terms",
                "go_terms": "GO Terms",
                "goterms": "GO Terms",
            }

            # Map key -> list of exact matches (entity_id, primary_alias)
            exact_matches: dict[
                tuple[str | None, str], list[tuple[int, Optional[str]]]
            ] = {}

            PrimaryAlias = aliased(EntityAlias)

            for th_norm, bucket in buckets.items():
                norms = [self._norm(b.word) for b in bucket]

                q = (
                    self.session.query(
                        EntityAlias.alias_norm.label("alias_norm"),
                        Entity.id.label("entity_id"),
                        PrimaryAlias.alias_value.label("primary_alias"),
                    )
                    .join(Entity, Entity.id == EntityAlias.entity_id)
                    .join(
                        PrimaryAlias,
                        (PrimaryAlias.entity_id == Entity.id)
                        & (PrimaryAlias.is_primary.is_(True)),
                    )
                    .filter(EntityAlias.alias_norm.in_(norms))
                )

                # filter by group if hint exists and is known
                if th_norm:
                    gname = hint_to_group_name.get(th_norm)
                    if gname and gname in group_name_to_id:
                        q = q.filter(Entity.group_id == group_name_to_id[gname])

                with bind.connect() as conn:
                    df = pd.read_sql(q.statement, conn)

                if df.empty:
                    continue

                for _, r in df.iterrows():
                    # We don't know which specific input word (if multiple share same norm),
                    # but hybrid resolution is applied per (th_norm, norm) anyway.
                    k = (th_norm, str(r["alias_norm"]))
                    exact_matches.setdefault(k, []).append(
                        (int(r["entity_id"]), r.get("primary_alias"))
                    )

            # Now decide per input: keep exact if exactly one match; else fallback to resolve_best
            for it in items:
                th_norm = type_hint_norm(it.type_hint)
                k = key_for(it.type_hint, it.word)
                matches = exact_matches.get(k, [])

                if len(matches) == 1:
                    eid, p = matches[0]
                    row = self._make_base_row(
                        resolver_mode=resolver_mode,
                        input_word=it.word,
                        input_type_hint=it.type_hint,
                        entity_id=eid,
                        primary_alias=p,
                        resolve_status="resolved",
                        resolve_method="exact",
                        resolve_score=1.0,
                        resolve_meta=None,
                        resolve_candidates=None,
                    )
                    resolved_rows.append(row)
                    continue

                # 0 or >1 => search fallback
                hints = self._hint_to_group_name(it.type_hint)
                res = resolver.resolve_best(it.word, entity_type_hints=hints)

                if res.best is None:
                    if emit_not_found_rows:
                        row = self._make_empty_row(resolver_mode, it.word, it.type_hint)
                        row["Resolve Status"] = getattr(res, "status", "not_found")
                        resolved_rows.append(row)
                    continue

                best = res.best
                meta = getattr(best, "meta", {}) or {}

                row = self._make_base_row(
                    resolver_mode=resolver_mode,
                    input_word=it.word,
                    input_type_hint=it.type_hint,
                    entity_id=int(best.entity_id),
                    primary_alias=None,  # filled later
                    resolve_status=getattr(res, "status", None),
                    resolve_method=getattr(best, "method", None),
                    resolve_score=(
                        float(getattr(best, "score", 0.0))
                        if getattr(best, "score", None) is not None
                        else None
                    ),
                    resolve_meta=json.dumps(meta) if meta else None,
                    resolve_candidates=(
                        self._serialize_candidates(res, candidates_top_n)
                        if include_candidates
                        else None
                    ),
                )
                resolved_rows.append(row)

        if not resolved_rows:
            self.logger.log("No entities resolved from inputs.", "WARNING")
            return pd.DataFrame()

        out_df = pd.DataFrame(resolved_rows)

        # Ensure neighbor cols exist
        for col in neighbor_cols:
            if col not in out_df.columns:
                out_df[col] = json.dumps([])

        # -----------------------------------------------------------------
        # Step 2) Enrich: entity_type + canonical_name + (ensure primary alias exists)
        # -----------------------------------------------------------------
        entity_ids = sorted({int(x) for x in out_df["Entity ID"].dropna().tolist()})

        PrimaryAlias = aliased(EntityAlias)
        stmt_meta = (
            select(
                Entity.id.label("entity_id"),
                EntityGroup.name.label("group_name"),
                PrimaryAlias.alias_value.label("primary_alias"),
            )
            .select_from(Entity)
            .join(EntityGroup, EntityGroup.id == Entity.group_id)
            .join(
                PrimaryAlias,
                (PrimaryAlias.entity_id == Entity.id)
                & (PrimaryAlias.is_primary.is_(True)),
            )
            .where(Entity.id.in_(entity_ids))
        )

        with bind.connect() as conn:
            meta_df = pd.read_sql(stmt_meta, conn)

        group_map = meta_df.set_index("entity_id")["group_name"].to_dict()
        prim_map = meta_df.set_index("entity_id")["primary_alias"].to_dict()

        out_df["Entity Type"] = out_df["Entity ID"].apply(
            lambda eid: (
                self._groupname_to_entity_type(group_map.get(int(eid), ""))
                if pd.notna(eid)
                else None
            )
        )

        # Fill primary alias if missing (common when resolved via search)
        def _fill_primary(row: pd.Series) -> Any:
            eid = row.get("Entity ID")
            if pd.isna(eid):
                return row.get("Primary Alias")
            if row.get("Primary Alias"):
                return row.get("Primary Alias")
            return prim_map.get(int(eid))

        out_df["Primary Alias"] = out_df.apply(_fill_primary, axis=1)

        # v1: Canonical Name = Primary Alias
        out_df["Canonical Name"] = out_df["Primary Alias"]

        # -----------------------------------------------------------------
        # Step 3) Aliases (top N or all)
        # -----------------------------------------------------------------
        stmt_aliases = select(
            EntityAlias.entity_id.label("entity_id"),
            EntityAlias.alias_value.label("alias_value"),
            EntityAlias.is_primary.label("is_primary"),
            EntityAlias.alias_type.label("alias_type"),
        ).where(EntityAlias.entity_id.in_(entity_ids))

        with bind.connect() as conn:
            alias_df = pd.read_sql(stmt_aliases, conn)

        alias_count_map = alias_df.groupby("entity_id")["alias_value"].count().to_dict()

        alias_df["primary_rank"] = alias_df["is_primary"].apply(
            lambda x: 0 if bool(x) else 1
        )
        alias_df["type_rank"] = alias_df["alias_type"].fillna("").astype(str)
        alias_df["val_rank"] = alias_df["alias_value"].fillna("").astype(str)
        alias_df = alias_df.sort_values(
            ["entity_id", "primary_rank", "type_rank", "val_rank"]
        )

        aliases_map: dict[int, list[str]] = {}
        for eid, g in alias_df.groupby("entity_id"):
            vals = g["alias_value"].dropna().astype(str).tolist()
            if not include_all_aliases:
                vals = vals[:aliases_top_n]
            aliases_map[int(eid)] = vals

        out_df["Alias Count"] = out_df["Entity ID"].apply(
            lambda eid: int(alias_count_map.get(int(eid), 0)) if pd.notna(eid) else 0
        )
        out_df["Aliases Top"] = out_df["Entity ID"].apply(
            lambda eid: (
                json.dumps(aliases_map.get(int(eid), []))
                if pd.notna(eid)
                else json.dumps([])
            )
        )

        # -----------------------------------------------------------------
        # Step 4) Neighborhood 1-hop (summarized) using EntityRelationship
        # -----------------------------------------------------------------
        ER = EntityRelationship
        stmt_edges = select(
            ER.entity_1_id.label("src_id"),
            ER.entity_2_id.label("dst_id"),
            ER.entity_1_group_id.label("src_group_id"),
            ER.entity_2_group_id.label("dst_group_id"),
        ).where(
            or_(
                ER.entity_1_id.in_(entity_ids),
                ER.entity_2_id.in_(entity_ids),
            )
        )

        with bind.connect() as conn:
            edges_df = pd.read_sql(stmt_edges, conn)

        if edges_df.empty:
            out_df["Degree Total (1-hop)"] = 0
            out_df["Degree By Type (1-hop)"] = json.dumps({})
        else:
            left = edges_df.rename(
                columns={
                    "src_id": "entity_id",
                    "dst_id": "neighbor_id",
                    "dst_group_id": "neighbor_group_id",
                }
            )[["entity_id", "neighbor_id", "neighbor_group_id"]]

            right = edges_df.rename(
                columns={
                    "dst_id": "entity_id",
                    "src_id": "neighbor_id",
                    "src_group_id": "neighbor_group_id",
                }
            )[["entity_id", "neighbor_id", "neighbor_group_id"]]

            nbr_df = pd.concat([left, right], ignore_index=True)
            nbr_df = nbr_df[nbr_df["entity_id"].isin(entity_ids)]
            nbr_df = nbr_df[nbr_df["entity_id"] != nbr_df["neighbor_id"]]

            if nbr_df.empty:
                out_df["Degree Total (1-hop)"] = 0
                out_df["Degree By Type (1-hop)"] = json.dumps({})
            else:
                NeighborPrimary = aliased(EntityAlias)
                neighbor_ids = sorted(set(nbr_df["neighbor_id"].tolist()))

                stmt_nmeta = (
                    select(
                        Entity.id.label("neighbor_id"),
                        EntityGroup.name.label("neighbor_group_name"),
                        NeighborPrimary.alias_value.label("neighbor_primary_name"),
                    )
                    .select_from(Entity)
                    .join(EntityGroup, EntityGroup.id == Entity.group_id)
                    .join(
                        NeighborPrimary,
                        (NeighborPrimary.entity_id == Entity.id)
                        & (NeighborPrimary.is_primary.is_(True)),
                    )
                    .where(Entity.id.in_(neighbor_ids))
                )

                with bind.connect() as conn:
                    nmeta = pd.read_sql(stmt_nmeta, conn)

                nmeta_map = nmeta.set_index("neighbor_id")[
                    ["neighbor_group_name", "neighbor_primary_name"]
                ].to_dict(orient="index")

                nbr_df["neighbor_group_name"] = nbr_df["neighbor_id"].apply(
                    lambda nid: nmeta_map.get(int(nid), {}).get("neighbor_group_name")
                )
                nbr_df["neighbor_name"] = nbr_df["neighbor_id"].apply(
                    lambda nid: nmeta_map.get(int(nid), {}).get("neighbor_primary_name")
                )
                nbr_df["neighbor_col"] = nbr_df["neighbor_group_name"].apply(
                    lambda gn: self._groupname_to_neighbor_col(gn) if gn else None
                )

                nbr_df = nbr_df.dropna(subset=["neighbor_col", "neighbor_name"])

                if nbr_df.empty:
                    out_df["Degree Total (1-hop)"] = 0
                    out_df["Degree By Type (1-hop)"] = json.dumps({})
                else:
                    nbr_df["neighbor_name"] = nbr_df["neighbor_name"].astype(str)
                    nbr_df = nbr_df.sort_values(
                        ["entity_id", "neighbor_col", "neighbor_name", "neighbor_id"]
                    )

                    nbr_df = nbr_df.groupby(
                        ["entity_id", "neighbor_col"], as_index=False
                    ).head(limit_neighbors_per_type)
                    nbr_df = nbr_df.groupby("entity_id", as_index=False).head(
                        limit_neighbors
                    )

                    agg = (
                        nbr_df.groupby(["entity_id", "neighbor_col"])["neighbor_name"]
                        .apply(list)
                        .reset_index()
                    )

                    deg_by_type = (
                        nbr_df.groupby(["entity_id", "neighbor_col"])["neighbor_id"]
                        .count()
                        .reset_index(name="deg")
                    )
                    deg_total = (
                        nbr_df.groupby("entity_id")["neighbor_id"]
                        .count()
                        .reset_index(name="degree_total")
                    )

                    deg_dict: dict[int, dict[str, int]] = {}
                    for _, r in deg_by_type.iterrows():
                        eid = int(r["entity_id"])
                        col = str(r["neighbor_col"])
                        deg_dict.setdefault(eid, {})[col] = int(r["deg"])

                    # Fill neighbor columns for all rows matching the same entity_id
                    for _, r in agg.iterrows():
                        eid = int(r["entity_id"])
                        col = str(r["neighbor_col"])
                        names = r["neighbor_name"]
                        out_df.loc[out_df["Entity ID"] == eid, col] = json.dumps(names)

                    # Merge degree_total
                    out_df = out_df.merge(
                        deg_total.rename(
                            columns={"degree_total": "Degree Total (1-hop)"}
                        ),
                        how="left",
                        left_on="Entity ID",
                        right_on="entity_id",
                        suffixes=("", "_new"),
                    )

                    # Cleanup merge helper column
                    if "entity_id" in out_df.columns:
                        out_df = out_df.drop(columns=["entity_id"])

                    # Coalesce: keep computed value if present, else fallback to existing (default 0)
                    if "Degree Total (1-hop)_new" in out_df.columns:
                        out_df["Degree Total (1-hop)"] = (
                            out_df["Degree Total (1-hop)_new"]
                            .fillna(out_df.get("Degree Total (1-hop)", 0))
                            .fillna(0)
                            .astype(int)
                        )
                        out_df = out_df.drop(columns=["Degree Total (1-hop)_new"])
                    else:
                        # If merge didn't add anything, ensure the column exists
                        if "Degree Total (1-hop)" not in out_df.columns:
                            out_df["Degree Total (1-hop)"] = 0
                        out_df["Degree Total (1-hop)"] = (
                            out_df["Degree Total (1-hop)"].fillna(0).astype(int)
                        )

                    out_df["Degree By Type (1-hop)"] = out_df["Entity ID"].apply(
                        lambda eid: (
                            json.dumps(deg_dict.get(int(eid), {}))
                            if pd.notna(eid)
                            else json.dumps({})
                        )
                    )

        # -----------------------------------------------------------------
        # Step 5) Output column filtering
        # -----------------------------------------------------------------
        output_columns = self.params.get("output_columns")
        if output_columns is not None:
            if isinstance(output_columns, str):
                output_columns = [output_columns]
            output_columns = [
                str(c).strip() for c in output_columns if c and str(c).strip()
            ]
            allowed = set(self.available_columns())
            unknown = [c for c in output_columns if c not in allowed]
            if unknown:
                self.logger.log(
                    f"Unknown output_columns: {unknown}. Allowed: {sorted(allowed)}",
                    "ERROR",
                )
                return pd.DataFrame()
            out_df = out_df[output_columns]

        return out_df

    # ---------------------------------------------------------------------
    # Row builders
    # ---------------------------------------------------------------------
    def _make_base_row(
        self,
        resolver_mode: str,
        input_word: str,
        input_type_hint: str | None,
        entity_id: int | None,
        primary_alias: str | None,
        resolve_status: str | None,
        resolve_method: str | None,
        resolve_score: float | None,
        resolve_meta: str | None,
        resolve_candidates: str | None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "Input Word": input_word,
            "Input Type Hint": input_type_hint,
            "Resolver Mode": resolver_mode,
            "Entity ID": entity_id,
            "Entity Type": None,
            "Canonical Name": None,
            "Primary Alias": primary_alias,
            "Aliases Top": json.dumps([]),
            "Alias Count": 0,
            "Degree Total (1-hop)": 0,
            "Degree By Type (1-hop)": json.dumps({}),
            "Resolve Status": resolve_status,
            "Resolve Method": resolve_method,
            "Resolve Score": resolve_score,
            "Resolve Meta": resolve_meta,
            "Resolve Candidates": resolve_candidates,
            "Genes": json.dumps([]),
            "Proteins": json.dumps([]),
            "Pathways": json.dumps([]),
            "GO Terms": json.dumps([]),
            "Diseases": json.dumps([]),
            "Chemicals": json.dumps([]),
        }
        return row

    def _make_empty_row(
        self, resolver_mode: str, input_word: str, input_type_hint: str | None
    ) -> dict[str, Any]:
        return self._make_base_row(
            resolver_mode=resolver_mode,
            input_word=input_word,
            input_type_hint=input_type_hint,
            entity_id=None,
            primary_alias=None,
            resolve_status="not_found",
            resolve_method=None,
            resolve_score=None,
            resolve_meta=None,
            resolve_candidates=None,
        )

    def _serialize_candidates(self, resolution: Any, top_n: int) -> str:
        """
        Serialize the top-N candidates into a compact JSON list.
        Fields chosen to be stable and useful for debugging/curation.
        """
        cands = getattr(resolution, "candidates", None)
        if not cands:
            return json.dumps([])

        out = []
        for c in list(cands)[:top_n]:
            meta = getattr(c, "meta", {}) or {}
            out.append(
                {
                    "entity_id": getattr(c, "entity_id", None),
                    "method": getattr(c, "method", None),
                    "score": getattr(c, "score", None),
                    "matched_name": getattr(c, "matched_name", None),
                    "group_id": meta.get("group_id"),
                    "pg_trgm_score": meta.get("pg_trgm_score"),
                    "xref_source": meta.get("xref_source"),
                    "alias_type": meta.get("alias_type"),
                    "locale": meta.get("locale"),
                }
            )
        return json.dumps(out)
