# biofilter/modules/db/models/model_variants.py
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Identity,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from biofilter.modules.db.base import Base, RetiredBase
from biofilter.modules.db.types import PKBigIntOrInt


class VariantConsequence(Base):
    """
    The Sequence Ontology terms VEP emits, in severity order.

    This table survived the cull of the 4.2.x lookups, and for a reason
    the others did not have: **the rank is information the annotation
    does not carry**. `variant_molecular_effects.consequence` is the term
    as VEP wrote it — `missense_variant`, `intron_variant` — and nothing
    in the string says one is worse than the other. Dropping this table
    removed the only ordering in the bundle, which is what two reports
    were using it for.

    Keyed by `name`, not a generated id. The 4.2.x shape pointed
    `consequence_id` at this table from 2.2 billion rows; the parquet
    carries the term itself, so the join is on the term.

    `consequence_group` and `consequence_category` are plain strings —
    the seed always held them that way, and the two tables that gave
    them ids existed only to satisfy foreign keys that no longer exist.

    Safe to join by name: compound VEP terms are split into atomic rows
    on the way in. Measured on chr22, 31 distinct terms, none compound,
    every one of them present in this seed.
    """

    __tablename__ = "variant_consequences"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    severity_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    consequence_group: Mapped[str | None] = mapped_column(String(64), nullable=True)  # noqa: E501
    consequence_category: Mapped[str | None] = mapped_column(String(64), nullable=True)  # noqa: E501
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)  # noqa: E501


class VariantImpact(Base):
    """
    VEP's four impact classes, in severity order.

    The same argument as `VariantConsequence`: `variant_molecular_effects`
    carries `impact` as HIGH / MODERATE / LOW / MODIFIER, and their order
    is convention rather than anything a string comparison recovers —
    sorted alphabetically, `HIGH` comes after `LOW`.
    """

    __tablename__ = "variant_impacts"

    name: Mapped[str] = mapped_column(String(16), primary_key=True)
    severity_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


# Kept as classes, excluded from every bundle (`builder.RETIRED_TABLES`).
#
# They gave `group` and `category` an id for a foreign key that is gone;
# `VariantConsequence` carries both as strings now. The classes remain
# only because `modules/report/` imports them and is frozen until
# ADR-004 §2.2 replaces it — they hold no data a bundle needs.


class VariantConsequenceGroup(RetiredBase):
    __tablename__ = "variant_consequence_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)


class VariantConsequenceCategory(RetiredBase):
    __tablename__ = "variant_consequence_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)


class VariantBiotype(RetiredBase):
    """
    A name and a description, and the name is already a column of
    `variant_molecular_effects`. No ordering, nothing else — retired.
    """

    __tablename__ = "variant_biotypes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


def map_variant_masters(engine, metadata):
    """
    One row per ALT allele of the gnomAD **joint** callset (v4.1).

    This describes the parquet `dtp_variant_gnomad_joint` writes, column
    for column. It used to describe the 4.2.x relational table instead,
    and the two had four columns in common out of twenty-five — which
    `db verify --schema` reported as no drift at all, because it was
    comparing the models against the empty parent stub the export
    generated *from* the models.

    What changed and why:

    - `variant_id` is gone. Identity is the natural key, carried as
      `variant_key` (`chrom:pos:ref:alt`), because a surrogate is valid
      only inside the bundle that issued it.
    - `position_start`/`position_end` collapse to `position`. Every row
      had start == end, or end derivable as start + len(ref) - 1.
    - `ac`/`an`/`af` become `*_joint`, beside `*_exomes` and `*_genomes`.
      The joint callset reports all three and they are not the same
      number; an unqualified `af` would have to pick one silently.
    - `variant_type` and `allele_type` are gone — never produced.
    - The in-silico predictors moved to `variant_predictions`. They are
      not in the joint VCF at all: they are INFO fields of the exomes
      and genomes callsets, which a different DTP reads.
    """
    if "variant_masters" in metadata.tables:
        return metadata.tables["variant_masters"]

    return Table(
        "variant_masters",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # `chrom:pos:ref:alt` — the identity, and the join key every
        # other variant table carries.
        Column("variant_key", String(256), nullable=False),
        Column("rsid", String(32), nullable=True),
        Column("quality_filter", String(64), nullable=True),
        # Allele counts, frequencies and totals, per callset.
        Column("ac_exomes", BigInteger, nullable=True),
        Column("ac_genomes", BigInteger, nullable=True),
        Column("ac_joint", BigInteger, nullable=True),
        Column("af_exomes", Float, nullable=True),
        Column("af_genomes", Float, nullable=True),
        Column("af_joint", Float, nullable=True),
        Column("an_exomes", BigInteger, nullable=True),
        Column("an_genomes", BigInteger, nullable=True),
        Column("an_joint", BigInteger, nullable=True),
        Column("exomes_filters", String(128), nullable=True),
        Column("genomes_filters", String(128), nullable=True),
        # Filtering allele frequencies.
        Column("faf95_joint", Float, nullable=True),
        Column("faf99_joint", Float, nullable=True),
        Column("fafmax_faf95_max_joint", Float, nullable=True),
        Column("fafmax_faf95_max_gen_anc_joint", String(32), nullable=True),
        # Ancestry group with the maximum allele frequency.
        Column("grpmax_joint", String(32), nullable=True),
        Column("af_grpmax_joint", Float, nullable=True),
        # Homozygote counts, per callset.
        Column("nhomalt_exomes", BigInteger, nullable=True),
        Column("nhomalt_genomes", BigInteger, nullable=True),
        Column("nhomalt_joint", BigInteger, nullable=True),
        UniqueConstraint(
            "chromosome",
            "position",
            "reference_allele",
            "alternate_allele",
            name="uq_variant_masters_natkey",
        ),
    )


def map_variant_rsid(engine, metadata):
    """
    dbSNP identifiers for the alleles gnomAD publishes.

    The joint callset's ID column is empty on every record, so this is
    produced from the exome and genome VCFs, and captured *before* their
    AC filter — measured on chr21, only ~25% of rsIDs survive AC>=5, and
    the ones dropped are the rare variants that most need an identifier.
    That is why this is larger than `variant_masters` and why it is a
    table rather than a column on it.
    """
    if "variant_rsid" in metadata.tables:
        return metadata.tables["variant_rsid"]

    return Table(
        "variant_rsid",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        Column("rsid", String(32), nullable=False),
    )


def map_variant_alphamissense(engine, metadata):
    """
    AlphaMissense pathogenicity scores, one row per transcript.

    Replaces the 4.2.x `variant_effect_predictions`: the same shape,
    keyed by `chrom:pos:ref:alt` instead of a generated `variant_id`.
    """
    if "variant_alphamissense" in metadata.tables:
        return metadata.tables["variant_alphamissense"]

    return Table(
        "variant_alphamissense",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # `predictor:version:transcript`, so a transcript-level and a
        # variant-level predictor can share the table.
        Column("predictor_key", String(128), nullable=False),
        Column("transcript_id", String(32), nullable=True),
        Column("predictor_name", String(64), nullable=False),
        Column("predictor_version", String(32), nullable=True),
        Column("score", Float, nullable=True),
        Column("classification", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", BigInteger, nullable=True),
        Column("etl_package_id", BigInteger, nullable=True),
    )


def map_variant_gtex(engine, metadata):
    """
    GTEx cis-QTL evidence, one row per variant x gene x tissue.

    Replaces the 4.2.x `variant_gene_regulatory_evidence`, keyed by the
    natural key. Which tissues are ingested is a config choice, not a
    property of the schema.
    """
    if "variant_gtex" in metadata.tables:
        return metadata.tables["variant_gtex"]

    return Table(
        "variant_gtex",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # `gene:qtl_type:tissue`.
        Column("evidence_key", String(256), nullable=False),
        Column("gene_id", String(32), nullable=False),
        Column("bio_context", String(128), nullable=True),
        Column("qtl_type", String(16), nullable=False),
        Column("beta", Float, nullable=True),
        Column("se", Float, nullable=True),
        Column("p_value", Float, nullable=True),
        Column("n", BigInteger, nullable=True),
        Column("effect_allele", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", BigInteger, nullable=True),
        Column("etl_package_id", BigInteger, nullable=True),
    )


def map_variant_predictions(engine, metadata):
    """
    In-silico predictor scores, one row per variant allele.

    These are INFO fields of gnomAD's exome and genome sites VCFs —
    beside the CSQ block, not inside it. The joint callset, which
    `variant_masters` comes from, does not publish them at all: its 664
    INFO fields are entirely frequency, count and QC. That is why they
    are here and not columns on `variant_masters`, where the 4.2.x model
    declared them and nothing ever filled them.

    A variant present in both the exome and genome callsets is read
    twice and deduped. Measured on chr22, all 901,714 such variants
    carry byte-identical scores in both — no disagreement, and no case
    of one callset scoring where the other is null — because a predictor
    is computed from the reference and the allele, not by the callset.
    A `callset` column here would have recorded which VCF a row happened
    to be read from, and multiplied rows on every join.

    All eight are site-level (`Number=1`) even where the underlying
    predictor is per transcript: `revel_max` is scored at the MANE
    Select or canonical transcript, `sift_max` and `polyphen_max` are
    maxima across transcripts.
    """
    if "variant_predictions" in metadata.tables:
        return metadata.tables["variant_predictions"]

    return Table(
        "variant_predictions",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # Deleteriousness.
        Column("cadd_raw_score", Float, nullable=True),
        Column("cadd_phred", Float, nullable=True),
        # Missense.
        Column("revel_max", Float, nullable=True),
        Column("sift_max", Float, nullable=True),
        Column("polyphen_max", Float, nullable=True),
        # Splicing.
        Column("spliceai_ds_max", Float, nullable=True),
        Column("pangolin_largest_ds", Float, nullable=True),
        # Conservation, across the 241 placental mammals of Zoonomia.
        Column("phylop", Float, nullable=True),
    )


def map_variant_molecular_effects(engine, metadata):
    """
    One row per (variant allele x transcript x consequence), from VEP.

    Describes the parquet `dtp_variant_gnomad_vep` writes. The 4.2.x
    shape carried `consequence_id`, `impact_id` and `biotype_id` into
    lookup tables and ranked severity on the way in; the parquet carries
    the strings VEP emitted and leaves ranking to whoever asks. That is
    why those lookup tables left the schema.

    `canonical`, `mane_select` and `mane_plus_clinical` are strings, not
    booleans: VEP writes `YES` or nothing, and a null that means "not
    canonical" is not the same as a false.
    """
    if "variant_molecular_effects" in metadata.tables:
        return metadata.tables["variant_molecular_effects"]

    return Table(
        "variant_molecular_effects",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("position", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        Column("variant_key", String(256), nullable=False),
        # No `callset` column. It recorded which VCF a row was read
        # from, not a property of the annotation. A variant present in
        # both is annotated once, from whichever the `precedence`
        # setting reads first: measured on chr22, of 3,039 alleles in
        # both callsets 3,038 carry byte-identical VEP output, and the
        # exception is the same consequence terms on a RefSeq rather
        # than an Ensembl transcript. Which callset held a variant is
        # already in `variant_masters`, as ac_exomes and ac_genomes.
        Column("allele", String(256), nullable=True),
        Column("consequence", String(255), nullable=True),
        Column("impact", String(32), nullable=True),
        # Gene and transcript, as VEP names them.
        Column("symbol", String(64), nullable=True),
        Column("gene", String(32), nullable=True),
        Column("feature_type", String(32), nullable=True),
        Column("feature", String(32), nullable=True),
        Column("biotype", String(64), nullable=True),
        Column("symbol_source", String(32), nullable=True),
        Column("hgnc_id", String(32), nullable=True),
        Column("strand", String(4), nullable=True),
        Column("variant_class", String(16), nullable=True),
        # Transcript selection flags: 'YES' or null.
        Column("canonical", String(8), nullable=True),
        Column("mane_select", String(32), nullable=True),
        Column("mane_plus_clinical", String(32), nullable=True),
        # Position within the feature, and HGVS/protein context.
        Column("exon", String(32), nullable=True),
        Column("intron", String(32), nullable=True),
        Column("hgvsc", String(128), nullable=True),
        Column("hgvsp", String(128), nullable=True),
        Column("amino_acids", String(32), nullable=True),
        Column("codons", String(64), nullable=True),
        Column("ensp", String(32), nullable=True),
        # LOFTEE.
        Column("lof", String(8), nullable=True),
        Column("lof_filter", String(128), nullable=True),
        Column("lof_flags", String(256), nullable=True),
        Column("lof_info", Text, nullable=True),
    )


def map_variant_effect_predictions(engine, metadata):
    """
    VariantEffectPredictions (BF4 4.1.0):
    - One row per predictor per variant (optionally per transcript)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    - No physical FK constraints
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_effect_predictions" in metadata.tables:
        return metadata.tables["variant_effect_predictions"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("predictor_key", String(128), nullable=False),
        Column("transcript_id", String(32), nullable=True),
        Column("predictor_name", String(64), nullable=False),
        Column("predictor_version", String(32), nullable=True),
        Column("score", Float, nullable=True),
        Column("classification", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_effect_predictions = Table(
            "variant_effect_predictions",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "predictor_key",
                name="pk_variant_effect_predictions",
            ),
        )
    else:
        variant_effect_predictions = Table(
            "variant_effect_predictions",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "predictor_key",
                name="pk_variant_effect_predictions",
            ),
        )

    return variant_effect_predictions


def map_variant_regulatory_elements(engine, metadata):
    """
    VariantRegulatoryElements (BF4 4.1.0):
    - One row per (variant × regulatory element × bio_context)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_regulatory_elements" in metadata.tables:
        return metadata.tables["variant_regulatory_elements"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("reg_element_key", String(192), nullable=False),
        Column("regulatory_element_id", String(64), nullable=False),
        Column("element_type", String(32), nullable=True),
        Column("bio_context", String(128), nullable=True),
        Column("score", Float, nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_regulatory_elements = Table(
            "variant_regulatory_elements",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "reg_element_key",
                name="pk_variant_regulatory_elements",
            ),
        )
    else:
        variant_regulatory_elements = Table(
            "variant_regulatory_elements",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "reg_element_key",
                name="pk_variant_regulatory_elements",
            ),
        )

    return variant_regulatory_elements


def map_variant_gene_regulatory_evidence(engine, metadata):
    """
    VariantGeneRegulatoryEvidence (BF4 4.1.0):
    - One row per (variant × gene × qtl_type × bio_context)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_gene_regulatory_evidence" in metadata.tables:
        return metadata.tables["variant_gene_regulatory_evidence"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("evidence_key", String(256), nullable=False),
        Column("gene_id", String(32), nullable=False),
        Column("bio_context", String(128), nullable=True),
        Column("qtl_type", String(16), nullable=False),
        Column("beta", Float, nullable=True),
        Column("se", Float, nullable=True),
        Column("p_value", Float, nullable=True),
        Column("n", Integer, nullable=True),
        Column("effect_allele", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_gene_regulatory_evidence = Table(
            "variant_gene_regulatory_evidence",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "evidence_key",
                name="pk_variant_gene_regulatory_evidence",
            ),
        )
    else:
        variant_gene_regulatory_evidence = Table(
            "variant_gene_regulatory_evidence",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "evidence_key",
                name="pk_variant_gene_regulatory_evidence",
            ),
        )

    return variant_gene_regulatory_evidence




class VariantGWAS(Base):
    """
    Flat representation of GWAS Catalog associations.

    This table hosts the raw + mapped data from the GWAS Catalog,
    joined with the EFO trait mapping file. It allows queries
    on variants, studies, and traits, even before full Entity integration.

    Future: link `variant_id`, `trait_id`, and `study_id` to Entities.
    """

    __tablename__ = "variant_gwas"

    # No surrogate id. The DTP writes this table straight to parquet with
    # no database in the path, so an autoincrement column would be a
    # promise nothing keeps — and the rows are associations, identified
    # by (pubmed_id, mapped_trait_id, snp_id), not by a counter.

    # Publication / study info
    pubmed_id = Column(BigInteger, index=True, nullable=True)
    # first_author = Column(String(255), nullable=True)
    # publication_date = Column(String(50), nullable=True)  # raw string for now  # noqa E501
    # journal = Column(String(255), nullable=True)
    # study_title = Column(Text, nullable=True)
    # link = Column(String(500), nullable=True)

    # Trait / phenotype mapping
    raw_trait = Column(String(255), nullable=True)  # "DISEASE/TRAIT" field  # noqa E501
    mapped_trait = Column(String(255), nullable=True)  # "EFO term"
    mapped_trait_id = Column(String(255), nullable=True)  # "EFO/MONDO ID"
    parent_trait = Column(String(255), nullable=True)  # Parent term
    parent_trait_id = Column(String(255), nullable=True)  # Parent URI ID

    # Variant info
    chr_id = Column(String(255), nullable=True)
    chr_pos = Column(Integer, nullable=True)
    reported_gene = Column(String(255), nullable=True)
    mapped_gene = Column(String(255), nullable=True)
    snp_id = Column(String(255), index=True, nullable=True)  # dbSNP ID (rsID)
    # Position of this SNP within the association's original `SNPS`
    # field, which lists several for an interaction ("rs1 x rs2"). The
    # DTP explodes that field here; `variant_gwas_snp` used to hold it.
    snp_rank = Column(Integer, nullable=True)
    snp_risk_allele = Column(String(255), nullable=True)  # Qual a origem
    risk_allele_frequency = Column(Float, nullable=True)
    context = Column(String(255), nullable=True)
    intergenic = Column(String(255), nullable=True)

    # Statistics
    p_value = Column(Float, nullable=True)
    pvalue_mlog = Column(Float, nullable=True)
    odds_ratio_beta = Column(String(255), nullable=True)
    ci_text = Column(String(255), nullable=True)  # confidence interval raw

    # Sample sizes
    initial_sample_size = Column(Text, nullable=True)
    replication_sample_size = Column(Text, nullable=True)

    # Platform info
    platform = Column(String(255), nullable=True)

    # `cnv` and `notes` are gone: the GWAS Catalog's CNV column is read
    # and dropped during the merge, and `notes` never had a writer.

    # Provenance (no FK), matching the other variant tables. This one
    # declared real constraints while its siblings did not, which stopped
    # meaning anything once the variant branch began writing parquet with
    # no database present — a constraint nothing can enforce is worse than
    # none, because it suggests a guarantee that is not there.
    data_source_id = Column(Integer, nullable=True)
    etl_package_id = Column(Integer, nullable=True)

    # The parquet has no id column, so tell the mapper what identifies a
    # row instead of letting it fail for want of a primary key. An
    # association is a study, a trait and one of its SNPs.
    __mapper_args__ = {
        "primary_key": [pubmed_id, mapped_trait_id, snp_id, snp_rank],
    }



# `variant_gwas_snp` is gone.
#
# It was an rsID index over `variant_gwas`, built during the load step by
# splitting the GWAS Catalog's `SNPS` field ("rs6934929 x rs7276462").
# The variant branch has no load step under ADR-003 and the DTP explodes
# that field into `variant_gwas` itself, carrying `snp_rank`. Its foreign
# key also pointed at a `variant_gwas.id` the parquet never had.
