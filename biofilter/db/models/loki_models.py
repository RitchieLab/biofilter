from biofilter.db.base import Base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    PrimaryKeyConstraint,
    BigInteger,
    Index,
)


class Setting(Base):
    __tablename__ = "setting"

    setting = Column(String(32), primary_key=True, nullable=False)
    value = Column(String(256))


class GrchUcschg(Base):
    __tablename__ = "grch_ucschg"

    grch = Column(Integer, primary_key=True)
    ucschg = Column(Integer, nullable=False)


class LdProfile(Base):
    __tablename__ = "ldprofile"

    ldprofile_id = Column(Integer, primary_key=True, autoincrement=True)
    ldprofile = Column(String(32), unique=True, nullable=False)
    description = Column(String(128))
    metric = Column(String(32))
    value = Column(Float)

    def __repr__(self):
        return f"<LdProfile(ldprofile='{self.ldprofile}', metric='{self.metric}', value={self.value})>"  # noqa E501


class Namespace(Base):
    __tablename__ = "namespace"

    namespace_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    namespace = Column(String(32), unique=True, nullable=False)
    polygenic = Column(Boolean, nullable=False, default=False)


class Relationship(Base):
    __tablename__ = "relationship"

    relationship_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )
    relationship = Column(String(32), unique=True, nullable=False)

    def __repr__(self):
        return f"<Relationship(relationship='{self.relationship}')>"


class Role(Base):
    __tablename__ = "role"

    role_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    role = Column(String(32), unique=True, nullable=False)
    description = Column(String(128))
    coding = Column(Boolean)
    exon = Column(Boolean)

    def __repr__(self):
        return f"<Role(role='{self.role}', coding={self.coding}, exon={self.exon})>"  # noqa E501


class Source(Base):
    __tablename__ = "source"

    source_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    source = Column(String(32), unique=True, nullable=False)
    updated = Column(DateTime)
    version = Column(String(32))
    grch = Column(Integer)
    ucschg = Column(Integer)
    current_ucschg = Column(Integer)
    last_status = Column(Boolean, default=False)

    def __repr__(self):
        return f"<Source(source='{self.source}', version='{self.version}')>"


class SourceOption(Base):
    __tablename__ = "source_option"

    source_id = Column(Integer, nullable=False)
    option = Column(String(32), nullable=False)
    value = Column(String(64))

    __table_args__ = (PrimaryKeyConstraint("source_id", "option"),)

    def __repr__(self):
        return f"<SourceOption(source_id={self.source_id}, option='{self.option}', value='{self.value}')>"  # noqa E501


class SourceFile(Base):
    __tablename__ = "source_file"

    source_id = Column(Integer, nullable=False)
    filename = Column(String(256), nullable=False)
    size = Column(BigInteger)
    modified = Column(DateTime)
    md5 = Column(String(64))

    __table_args__ = (PrimaryKeyConstraint("source_id", "filename"),)

    def __repr__(self):
        return f"<SourceFile(source_id={self.source_id}, filename='{self.filename}')>"  # noqa E501


class Type(Base):
    __tablename__ = "type"

    type_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    type = Column(String(32), unique=True, nullable=False)

    def __repr__(self):
        return f"<Type(type='{self.type}')>"


class Warning(Base):
    __tablename__ = "warning"

    warning_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    source_id = Column(Integer, nullable=False)
    warning = Column(String(8192))

    def __repr__(self):
        return f"<Warning(source_id={self.source_id}, warning_len={len(self.warning) if self.warning else 0})>"  # noqa E501

    __table_args__ = (Index("warning__source", "source_id"),)


# SNPS
class SnpMerge(Base):
    __tablename__ = "snp_merge"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rsMerged = Column(Integer, nullable=False)
    rsCurrent = Column(Integer, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("snp_merge__merge_current", "rsMerged", "rsCurrent"),
    )  # noqa E501

    def __repr__(self):
        return f"<SnpMerge(rsMerged={self.rsMerged}, rsCurrent={self.rsCurrent})>"  # noqa E501


class SnpLocus(Base):
    __tablename__ = "snp_locus"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rs = Column(Integer, nullable=False)
    chr = Column(Integer, nullable=False)
    pos = Column(BigInteger, nullable=False)
    validated = Column(Boolean, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("snp_locus__rs_chr_pos", "rs", "chr", "pos"),
        Index("snp_locus__chr_pos_rs", "chr", "pos", "rs"),
    )

    def __repr__(self):
        return f"<SnpLocus(rs={self.rs}, chr={self.chr}, pos={self.pos})>"


class SnpEntrezRole(Base):
    __tablename__ = "snp_entrez_role"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rs = Column(Integer, nullable=False)
    entrez_id = Column(Integer, nullable=False)
    role_id = Column(Integer, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("snp_entrez_role__rs_entrez_role", "rs", "entrez_id", "role_id"),
    )

    def __repr__(self):
        return f"<SnpEntrezRole(rs={self.rs}, entrez_id={self.entrez_id}, role_id={self.role_id})>"  # noqa E501


class SnpBiopolymerRole(Base):
    __tablename__ = "snp_biopolymer_role"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rs = Column(Integer, nullable=False)
    biopolymer_id = Column(Integer, nullable=False)
    role_id = Column(Integer, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index(
            "snp_biopolymer_role__rs_biopolymer_role",
            "rs",
            "biopolymer_id",
            "role_id",  # noqa E501
        ),
        Index(
            "snp_biopolymer_role__biopolymer_rs_role",
            "biopolymer_id",
            "rs",
            "role_id",  # noqa E501
        ),
    )

    def __repr__(self):
        return f"<SnpBiopolymerRole(rs={self.rs}, biopolymer_id={self.biopolymer_id}, role_id={self.role_id})>"  # noqa E501


# BIOPOLYMERS
class Biopolymer(Base):
    __tablename__ = "biopolymer"

    biopolymer_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )
    type_id = Column(Integer, nullable=False)
    label = Column(String(64), nullable=False)
    description = Column(String(256))
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("biopolymer__type", "type_id"),
        Index("biopolymer__label_type", "label", "type_id"),
    )

    def __repr__(self):
        return f"<Biopolymer(label='{self.label}', type_id={self.type_id})>"


class BiopolymerName(Base):
    __tablename__ = "biopolymer_name"

    biopolymer_id = Column(Integer, nullable=False)
    namespace_id = Column(Integer, nullable=False)
    name = Column(String(256), nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("biopolymer_id", "namespace_id", "name"),
        Index(
            "biopolymer_name__name_namespace_biopolymer",
            "name",
            "namespace_id",
            "biopolymer_id",
        ),
    )

    def __repr__(self):
        return f"<BiopolymerName(name='{self.name}', namespace_id={self.namespace_id})>"  # noqa E501


class BiopolymerNameName(Base):
    __tablename__ = "biopolymer_name_name"

    namespace_id = Column(Integer, nullable=False)
    name = Column(String(256), nullable=False)
    type_id = Column(Integer, nullable=False)
    new_namespace_id = Column(Integer, nullable=False)
    new_name = Column(String(256), nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "new_namespace_id", "new_name", "type_id", "namespace_id", "name"
        ),
    )

    def __repr__(self):  # noqa E501
        return f"<BiopolymerNameName({self.namespace_id}:{self.name} → {self.new_namespace_id}:{self.new_name})>"  # noqa E501


class BiopolymerRegion(Base):
    __tablename__ = "biopolymer_region"

    biopolymer_id = Column(Integer, nullable=False)
    ldprofile_id = Column(Integer, nullable=False)
    chr = Column(Integer, nullable=False)
    posMin = Column(BigInteger, nullable=False)
    posMax = Column(BigInteger, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "biopolymer_id", "ldprofile_id", "chr", "posMin", "posMax"
        ),
        Index(
            "biopolymer_region__ldprofile_chr_min", "ldprofile_id", "chr", "posMin"  # noqa E501
        ),  # noqa E501
        Index(
            "biopolymer_region__ldprofile_chr_max", "ldprofile_id", "chr", "posMax"  # noqa E501
        ),  # noqa E501
    )

    def __repr__(self):
        return f"<BiopolymerRegion(chr={self.chr}, posMin={self.posMin}, posMax={self.posMax})>"  # noqa E501


class BiopolymerZone(Base):
    __tablename__ = "biopolymer_zone"

    biopolymer_id = Column(Integer, nullable=False)
    chr = Column(Integer, nullable=False)
    zone = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("biopolymer_id", "chr", "zone"),
        Index("biopolymer_zone__zone", "chr", "zone", "biopolymer_id"),
    )

    def __repr__(self):
        return f"<BiopolymerZone(chr={self.chr}, zone={self.zone})>"


# GROUPS
class Group(Base):
    __tablename__ = "group"

    group_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    type_id = Column(Integer, nullable=False)
    label = Column(String(64), nullable=False)
    description = Column(String(256))
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("group__type", "type_id"),
        Index("group__label_type", "label", "type_id"),
    )

    def __repr__(self):
        return f"<Group(label='{self.label}', type_id={self.type_id})>"


class GroupName(Base):
    __tablename__ = "group_name"

    group_id = Column(Integer, nullable=False)
    namespace_id = Column(Integer, nullable=False)
    name = Column(String(256), nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("group_id", "namespace_id", "name"),
        Index(
            "group_name__name_namespace_group", "name", "namespace_id", "group_id"  # noqa E501
        ),  # noqa E501
        Index("group_name__source_name", "source_id", "name"),
    )

    def __repr__(self):
        return f"<GroupName(name='{self.name}', namespace_id={self.namespace_id})>"  # noqa E501


class GroupGroup(Base):
    __tablename__ = "group_group"

    group_id = Column(Integer, nullable=False)
    related_group_id = Column(Integer, nullable=False)
    relationship_id = Column(Integer, nullable=False)
    direction = Column(Integer, nullable=False)
    contains = Column(Boolean)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "group_id", "related_group_id", "relationship_id", "direction"
        ),
        Index("group_group__related", "related_group_id", "group_id"),
    )

    def __repr__(self):
        return f"<GroupGroup(group_id={self.group_id}, related_group_id={self.related_group_id})>"  # noqa E501


class GroupBiopolymer(Base):
    __tablename__ = "group_biopolymer"

    group_id = Column(Integer, nullable=False)
    biopolymer_id = Column(Integer, nullable=False)
    specificity = Column(Integer, nullable=False)
    implication = Column(Integer, nullable=False)
    quality = Column(Integer, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("group_id", "biopolymer_id", "source_id"),
        Index("group_biopolymer__biopolymer", "biopolymer_id", "group_id"),
    )

    def __repr__(self):
        return f"<GroupBiopolymer(group_id={self.group_id}, biopolymer_id={self.biopolymer_id})>"  # noqa E501


class GroupMemberName(Base):
    __tablename__ = "group_member_name"

    group_id = Column(Integer, nullable=False)
    member = Column(Integer, nullable=False)
    type_id = Column(Integer, nullable=False)
    namespace_id = Column(Integer, nullable=False)
    name = Column(String(256), nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "group_id", "member", "type_id", "namespace_id", "name"
        ),  # noqa E501
    )

    def __repr__(self):
        return f"<GroupMemberName(group_id={self.group_id}, name='{self.name}')>"  # noqa E501


# GWAS
class Gwas(Base):
    __tablename__ = "gwas"

    gwas_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    rs = Column(Integer)
    chr = Column(Integer)
    pos = Column(BigInteger)
    trait = Column(String(256), nullable=False)
    snps = Column(String(256))
    orbeta = Column(String(8))
    allele95ci = Column(String(16))
    riskAfreq = Column(String(16))  # Corrigido de VARCAHR para VARCHAR
    pubmed_id = Column(Integer)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("gwas__rs", "rs"),
        Index("gwas__chr_pos", "chr", "pos"),
    )

    def __repr__(self):
        return f"<Gwas(trait='{self.trait}', rs={self.rs}, chr={self.chr}, pos={self.pos})>"  # noqa E501


class Chain(Base):
    __tablename__ = "chain"

    chain_id = Column(
        Integer, primary_key=True, autoincrement=True, nullable=False
    )  # noqa E501
    old_ucschg = Column(Integer, nullable=False)
    old_chr = Column(Integer, nullable=False)
    old_start = Column(BigInteger, nullable=False)
    old_end = Column(BigInteger, nullable=False)
    new_ucschg = Column(Integer, nullable=False)
    new_chr = Column(Integer, nullable=False)
    new_start = Column(BigInteger, nullable=False)
    new_end = Column(BigInteger, nullable=False)
    score = Column(BigInteger, nullable=False)
    is_fwd = Column(Boolean, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        Index("chain__oldhg_newhg_chr", "old_ucschg", "new_ucschg", "old_chr"),
    )

    def __repr__(self):
        return f"<Chain(chain_id={self.chain_id}, old_chr={self.old_chr}, new_chr={self.new_chr})>"  # noqa E501


class ChainData(Base):
    __tablename__ = "chain_data"

    chain_id = Column(Integer, nullable=False)
    old_start = Column(BigInteger, nullable=False)
    old_end = Column(BigInteger, nullable=False)
    new_start = Column(BigInteger, nullable=False)
    source_id = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("chain_id", "old_start"),
        Index("chain_data__end", "chain_id", "old_end"),
    )

    def __repr__(self):
        return f"<ChainData(chain_id={self.chain_id}, old_start={self.old_start}, old_end={self.old_end})>"  # noqa E501


"""
================================================================================
Developer Note  Transitional Models from LOKI (APSW) to Biofilter 3R
================================================================================

This module contains **temporary models** that mirror the legacy structure of
the original LOKI system, which used APSW (SQLite wrapper) for direct SQL
access.

Purpose:
- These SQLAlchemy-based models were introduced to support the transitional
    development phase of Biofilter version 3R (Refactor, Replace, Rebuild).
- They allow for consistent interaction with legacy data while the new 3R
    models and architecture are being finalized.

Key Characteristics:
- Models reflect the original structure and naming conventions from LOKI.
- Foreign keys, constraints, and advanced relationships have been omitted for
    performance and compatibility with the legacy flat schema.
- Data ingestion routines, lookups, and analyses can still reference these
    models during migration and testing stages.

Deprecation Notice:
- These models will be deprecated/removed after the release of Biofilter 3R.
- All data and functionality will be fully ported to the new canonical schema,
    which provides typed relationships, entity normalization, and data lineage
    tracking.

Migration Strategy:
- Developers are encouraged to begin transitioning all ETL logic and downstream
    processing to use the new models in `biofilter/models/entity/`, `source/`,
    and `ontology/`.
- During the interim, these models will serve as a compatibility bridge only.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
