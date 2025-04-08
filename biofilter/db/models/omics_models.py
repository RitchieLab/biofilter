from sqlalchemy import Column, Integer, String

# from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


# GENE TABLES
class GeneGroup(Base):
    __tablename__ = "gene_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)


class Gene(Base):
    __tablename__ = "genes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)

    hgnc_id = Column(String, unique=True, nullable=True)
    entrez_id = Column(String, nullable=True)
    ensembl_id = Column(String, nullable=True)

    chromosome = Column(String, nullable=True)  # Ex: "1", "X", "MT"
    start = Column(Integer, nullable=True)
    end = Column(Integer, nullable=True)
    strand = Column(String, nullable=True)  # "+", "-"

    locus_group = Column(String, nullable=True)
    locus_type = Column(String, nullable=True)

    gene_group_id = Column(Integer, nullable=True)

    data_source_id = Column(Integer, nullable=True)

    # created_at = Column(DateTime, default=utcnow)
    # updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


"""
================================================================================
Developer Note - Gene Model (Biofilter 3R)
================================================================================

This model represents canonical gene metadata aligned with HGNC and similar
databases, and is integrated with the entity system via `entity_id`.

Design Considerations:

- This table serves as a simplified, centralized structure for storing gene
    identifiers (HGNC, Entrez, Ensembl) and basic genomic location data.
- Chromosome position fields (`chromosome`, `start`, `end`, `strand`) are
    included directly in this table for simplicity and performance in queries.

Rationale for Inlined Locus Data:

- Although some systems represent gene loci in a separate table, this model
  assumes **a 1:1 relationship between a gene and its canonical location**.
- As of now, we do not support multiple loci per gene.
- If required in the future (e.g., for alternate assemblies or sources),
    a separate `GeneLocus` model can be introduced, with support for:

    - Gene ID reference
    - Assembly version (e.g., GRCh38, GRCh37)
    - Source system (e.g., Ensembl, UCSC)
    - Locus-specific metadata

This would allow a 1:N relationship between `Gene` and `GeneLocus`, and could
enable support for comparative genomics or dual-assembly mapping.

Future Improvements:

- Consider enforcing relationships via FK once the ingestion workflow
    stabilizes.
- Track source/versioning explicitly using `data_source_id` and optional audit
    fields.
- Add support for alternate loci, scaffolds, and assembly-specific views if
    needed.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
