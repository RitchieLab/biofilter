from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import biofilter.modules.etl.dtps.dtp_clingen as mod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, msg: str, level: str = "INFO"):
        self.messages.append((level, msg))


@dataclass
class FakeSourceSystem:
    name: str


@dataclass
class FakeDataSource:
    name: str
    source_system: FakeSourceSystem
    id: int = 91


@dataclass
class FakePackage:
    id: int = 701


@dataclass
class FakeRow:
    id: int


class FakeGroupQuery:
    def __init__(self, session):
        self.session = session
        self.name = None

    def filter_by(self, **kwargs):
        self.name = kwargs.get("name")
        return self

    def first(self):
        return self.session.groups.get(self.name)


class FakeRelationshipTypeQuery:
    def __init__(self, session):
        self.session = session
        self.code = None

    def filter_by(self, **kwargs):
        self.code = kwargs.get("code")
        return self

    def first(self):
        return self.session.relationship_types.get(self.code)


class FakeAliasQuery:
    def __init__(self, session):
        self.session = session

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        if self.session.alias_query_results:
            return self.session.alias_query_results.pop(0)
        return []


class FakeRelationshipDeleteQuery:
    def __init__(self, session):
        self.session = session
        self.data_source_id = None

    def filter_by(self, **kwargs):
        self.data_source_id = kwargs.get("data_source_id")
        return self

    def delete(self, synchronize_session=False):
        self.session.delete_called = True
        self.session.deleted_data_source_id = self.data_source_id
        return self.session.deleted_count


class FakeSession:
    def __init__(
        self,
        *,
        groups: dict[str, object],
        relationship_types: dict[str, object],
        alias_query_results: list[list[tuple]],
        deleted_count: int = 0,
    ):
        self.groups = groups
        self.relationship_types = relationship_types
        self.alias_query_results = alias_query_results
        self.deleted_count = deleted_count

        self.delete_called = False
        self.deleted_data_source_id = None
        self.bulk_insert_called = False
        self.inserted_records = []
        self.commit_count = 0
        self.rollback_count = 0

    def query(self, *entities):
        if len(entities) == 1 and entities[0] is mod.EntityGroup:
            return FakeGroupQuery(self)
        if len(entities) == 1 and entities[0] is mod.EntityRelationshipType:
            return FakeRelationshipTypeQuery(self)
        if len(entities) == 1 and entities[0] is mod.EntityRelationship:
            return FakeRelationshipDeleteQuery(self)

        # Query of EntityAlias columns
        if len(entities) == 3 and all(
            getattr(e, "class_", None) is mod.EntityAlias for e in entities
        ):
            return FakeAliasQuery(self)

        raise AssertionError(f"Unexpected query entities: {entities}")

    def bulk_insert_mappings(self, model, records):
        assert model is mod.EntityRelationship
        self.bulk_insert_called = True
        self.inserted_records.extend(records)

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


def _write_processed_file(tmp_path: Path, rows: list[dict]) -> tuple[str, FakeDataSource]:
    raw = tmp_path / "processed"
    ss = FakeSourceSystem(name="CLINGEN")
    ds = FakeDataSource(name="clingen", source_system=ss)
    target_dir = raw / ss.name / ds.name
    target_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(target_dir / "gene_disease_validity.parquet", index=False)
    return str(raw), ds


def _make_dtp(session: FakeSession, ds: FakeDataSource) -> mod.DTP:
    dtp = mod.DTP(
        logger=DummyLogger(),
        datasource=ds,
        package=FakePackage(),
        session=session,
    )
    dtp.check_compatibility = lambda: None
    return dtp


def test_load_fails_when_relationship_type_missing(tmp_path):
    processed_dir, ds = _write_processed_file(
        tmp_path,
        [
            {
                "hgnc_id": "HGNC:5",
                "gene_symbol": "GENE5",
                "mondo_id": "MONDO:0001",
                "disease_label": "Disease A",
            }
        ],
    )

    session = FakeSession(
        groups={"Genes": FakeRow(2), "Diseases": FakeRow(7)},
        relationship_types={},  # missing 'part_of'
        alias_query_results=[],
    )
    dtp = _make_dtp(session, ds)

    ok, msg = dtp.load(processed_dir=processed_dir)

    assert ok is False
    assert "Relationship type 'part_of' not found" in msg
    assert session.delete_called is False
    assert session.bulk_insert_called is False


def test_load_aborts_when_no_valid_rows_without_deleting(tmp_path):
    processed_dir, ds = _write_processed_file(
        tmp_path,
        [
            {
                "hgnc_id": "HGNC:5",
                "gene_symbol": "GENE5",
                "mondo_id": "MONDO:0001",
                "disease_label": "Disease A",
            }
        ],
    )

    session = FakeSession(
        groups={"Genes": FakeRow(2), "Diseases": FakeRow(7)},
        relationship_types={"part_of": FakeRow(1)},
        alias_query_results=[[], []],  # no mapping for genes/diseases
    )
    dtp = _make_dtp(session, ds)

    ok, msg = dtp.load(processed_dir=processed_dir)

    assert ok is False
    assert "No valid ClinGen relationships were resolved" in msg
    assert session.delete_called is False
    assert session.bulk_insert_called is False


def test_load_replaces_atomically_and_counts_inserted_rows(tmp_path):
    processed_dir, ds = _write_processed_file(
        tmp_path,
        [
            {
                "hgnc_id": "HGNC:5",
                "gene_symbol": "GENE5",
                "mondo_id": "MONDO:0001",
                "disease_label": "Disease A",
            },
            {
                # duplicate row on purpose, should be deduplicated before insert
                "hgnc_id": "HGNC:5",
                "gene_symbol": "GENE5",
                "mondo_id": "MONDO:0001",
                "disease_label": "Disease A",
            },
        ],
    )

    session = FakeSession(
        groups={"Genes": FakeRow(2), "Diseases": FakeRow(7)},
        relationship_types={"part_of": FakeRow(1)},
        alias_query_results=[
            [("HGNC:5", 101, 2)],  # genes map
            [("MONDO:0001", 202, 7)],  # diseases map
        ],
        deleted_count=3,
    )
    dtp = _make_dtp(session, ds)

    drop_calls = []
    create_calls = []
    dtp.drop_indexes = lambda spec: drop_calls.append(
        getattr(spec, "__name__", str(spec))
    )
    dtp.create_indexes = lambda spec: create_calls.append(
        getattr(spec, "__name__", str(spec))
    )

    ok, msg = dtp.load(processed_dir=processed_dir)

    assert ok is True
    assert "Total ClinGen Relationships: 1" in msg
    assert session.delete_called is True
    assert session.deleted_data_source_id == ds.id
    assert session.bulk_insert_called is True
    assert len(session.inserted_records) == 1
    assert session.commit_count == 1
    assert len(drop_calls) == 1
    assert len(create_calls) == 1
    assert "entity_relationships" in drop_calls[0]
    assert drop_calls[0] == create_calls[0]
