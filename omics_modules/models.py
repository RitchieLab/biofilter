from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, SmallInteger
from sqlalchemy.orm import relationship
import datetime
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# TABLES FOR DATA SOURCES CONTROL


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)  # Ex: dbSNP, Ensembl, UniProt
    data_type = Column(String, nullable=False)  # Ex: SNP, Gene, Protein
    source_url = Column(String, nullable=True)  # URL do data source
    format = Column(String, nullable=False)  # CSV, JSON, API, SQL Dump
    grch_version = Column(String, nullable=True)  # Ex: GRCh38, GRCh37
    ucschg_version = Column(String, nullable=True)  # Ex: hg19, hg38
    dtp_version = Column(String, nullable=False)  # Versão do Data Transformation Process
    last_update = Column(DateTime, nullable=True)  # Data da última atualização bem-sucedida
    last_status = Column(String, nullable=False, default="pending")  # "success", "failed", "running"
    active = Column(Boolean, default=True)  # Indica se o data source está ativo
    created_at = Column(DateTime, default=lambda: datetime.datetime.now(datetime.timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.datetime.now(datetime.timezone.utc), onupdate=lambda: datetime.datetime.now(datetime.timezone.utc))


class WorkProcess(Base):
    __tablename__ = "workprocesses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False)
    start_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="running")  # "pending", "running", "completed", "failed"
    error_message = Column(String, nullable=True)  # Se falhar, armazenar erro
    records_processed = Column(Integer, nullable=True, default=0)  # Registros inseridos/atualizados
    tables_updated = Column(String, nullable=True)  # Ex: "snps, genes, proteins"
    dtp_script = Column(String, nullable=False)  # Nome do script Python que rodou o ETL

    # Relacionamento com DataSource
    data_source = relationship("DataSource", back_populates="workprocesses")


# Adicionar relacionamento reverso no DataSource
DataSource.workprocesses = relationship("WorkProcess", back_populates="data_source", cascade="all, delete-orphan")


class ExtractedFile(Base):
    __tablename__ = "extracted_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    workprocess_id = Column(Integer, ForeignKey("workprocesses.id", ondelete="CASCADE"), nullable=False)
    file_name = Column(String, nullable=False)  # Nome do arquivo extraído
    file_path = Column(String, nullable=False)  # Caminho/localização do arquivo
    file_size = Column(Integer, nullable=True)  # Tamanho do arquivo em bytes
    extraction_date = Column(DateTime, default=datetime.datetime.utcnow)

    # Relacionamento com WorkProcess
    workprocess = relationship("WorkProcess", back_populates="extracted_files")


# Adicionar relacionamento reverso no WorkProcess
WorkProcess.extracted_files = relationship("ExtractedFile", back_populates="workprocess", cascade="all, delete-orphan")

# ===================================================================================================
# TABLES FOR OMICS DATA


class SNPMerge(Base):
    __tablename__ = "snpmerges"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Chave primária única
    rs_source = Column(Integer, nullable=False, index=True)  # Pode se repetir
    rs_current = Column(Integer, unique=True, index=True, nullable=False)  # Ex: 3007669
    source = Column(String, nullable=True)  # Ex: "dbSNP", "1000 Genomes"


class SNP(Base):
    __tablename__ = "snps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rs_source = Column(Integer, nullable=True, index=True)  # Ex: 9411893
    rs_current = Column(Integer, index=True, nullable=False)  # Ex: 3007669
    chromosome = Column(SmallInteger, nullable=False)  # Ex: 1-22, 23 (X), 24 (Y), 25 (MT)
    position = Column(Integer, nullable=False)  # SNP position in the chromosome
    reference_allele = Column(String, nullable=False)  # Ref Allele (ex: "A", "GTC", "")
    alternate_allele = Column(String, nullable=False)  # Alt Allele (ex: "G", "", "TCG")
    variation_type = Column(String, nullable=False)  # "SNP", "Insertion", "Deletion"
    build_source = Column(String, nullable=True)  # Ex: "GRCh38"
    valid = Column(Boolean, default=True)  # Indication if this SNP is currently valid
    source = Column(String, nullable=True)  # Ex: "dbSNP", "1000 Genomes"


class Gene(Base):
    __tablename__ = "genes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gene_id = Column(String, unique=True, index=True, nullable=False)  # Ex: ENSG00000157764
    symbol = Column(String, index=True, nullable=False)  # Ex: BRCA1
    chromosome = Column(SmallInteger, nullable=False)  # Ex: 1-22, 23=X, 24=Y, 25=MT
    start_position = Column(Integer, nullable=False)
    end_position = Column(Integer, nullable=False)
    strand = Column(String, nullable=False)  # "+" ou "-"
    description = Column(String, nullable=True)  # Ex: "Breast cancer 1 gene"
    source = Column(String, nullable=True)  # Ex: "Ensembl"


class Protein(Base):
    __tablename__ = "proteins"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uniprot_id = Column(String, unique=True, index=True, nullable=False)  # Ex: P04637 (UniProt ID)
    symbol = Column(String, index=True, nullable=False)  # Ex: p53
    # gene_id = Column(Integer, ForeignKey("genes.id"), nullable=True)  # Vai estar no Relacionamento
    sequence = Column(String, nullable=False)  # Sequência proteica
    description = Column(String, nullable=True)  # Ex: "Tumor suppressor protein"


class Metabolite(Base):
    __tablename__ = "metabolites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chebi_id = Column(String, unique=True, index=True, nullable=False)  # Ex: CHEBI:15422
    name = Column(String, index=True, nullable=False)  # Ex: ATP
    formula = Column(String, nullable=True)  # Ex: C10H16N5O13P3
    description = Column(String, nullable=True)  # Ex: "Adenosine triphosphate"

# ===================================================================================================
# TABLES FOR OMICS RELATIONSHIPS


class OmicsRelationship(Base):
    __tablename__ = "omics_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_1_id = Column(Integer, nullable=False)  # ID da primeira entidade
    entity_1_type = Column(String, nullable=False)  # Tipo da primeira entidade (Gene, SNP, Protein, etc.)
    entity_2_id = Column(Integer, nullable=False)  # ID da segunda entidade
    entity_2_type = Column(String, nullable=False)  # Tipo da segunda entidade
    relationship_type = Column(String, nullable=False)  # "is_a", "part_of", "regulates", "has_part"
    role = Column(String, nullable=True)  # Ex: "activator", "inhibitor", "binding_site"


class OmicsTermAlternative(Base):
    __tablename__ = "omics_term_alternatives"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)  # ID da entidade (Gene, SNP, Metabolito, etc.)
    term_1 = Column(String, nullable=False)  # Nome principal ou termo referência
    term_2 = Column(String, nullable=False)  # Nome alternativo ou relacionado
    term_type = Column(String, nullable=False)  # Tipo de relacionamento (official, alias, previous, synonym)
