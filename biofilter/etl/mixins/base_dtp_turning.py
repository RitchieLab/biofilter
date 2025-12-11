from sqlalchemy import text

# import time


class DBTuningMixin:
    """
    Mixin to apply database optimizations for bulk insert operations.
    Currently supports SQLite and creates indexes for SQLite/PostgreSQL.
    """

    def db_write_mode(self):
        """
        Apply SQLite-specific PRAGMA settings to optimize for bulk insert.
        Does nothing if not using SQLite.
        """
        if self.session.bind.dialect.name != "sqlite":
            return

        msg = "⚙️  Applying SQLite PRAGMA optimizations for bulk insert"
        self.logger.log(msg, "DEBUG")  # noqa E501

        # self.session.execute(text("PRAGMA journal_mode = WAL;"))
        self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        self.session.execute(text("PRAGMA synchronous = NORMAL;"))
        self.session.execute(text("PRAGMA locking_mode = EXCLUSIVE;"))
        self.session.execute(text("PRAGMA temp_store = MEMORY;"))
        self.session.execute(
            text("PRAGMA cache_size = -100000;")
        )  # ~100MB of memory cache  # noqa E501
        self.session.execute(
            text("PRAGMA foreign_keys = OFF;")
        )  # Temporarily disable FK checks  # noqa E501
        self.session.commit()

        # DEBUG MODE
        # mode = self.session.execute(text("PRAGMA journal_mode")).scalar()
        # self.logger.log(f"🧾 Current journal_mode: {mode}", "DEBUG")

    def db_read_mode(self):
        """
        Reset SQLite PRAGMAs to default values after bulk insert.
        Does nothing if not using SQLite.
        """
        if self.session.bind.dialect.name != "sqlite":
            return

        self.logger.log(
            "🔄 Resetting SQLite PRAGMAs to default settings", "DEBUG"
        )  # noqa E501

        self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        self.session.execute(text("PRAGMA synchronous = FULL;"))
        self.session.execute(text("PRAGMA locking_mode = NORMAL;"))
        self.session.execute(text("PRAGMA foreign_keys = ON;"))
        self.session.commit()

    # def db_read_mode(self):
    #     """
    #     Reset SQLite PRAGMAs to default values after bulk insert.
    #     Uses AUTOCOMMIT + checkpoint to avoid 'database is locked'.
    #     """
    #     if self.session.bind.dialect.name != "sqlite":
    #         return

    #     self.logger.log("🔄 Resetting SQLite PRAGMAs to default settings", "DEBUG")

    #     # garanta que não há transação ativa
    #     try:
    #         self.session.commit()
    #     except Exception:
    #         self.session.rollback()

    #     # use AUTOCOMMIT para PRAGMAs que exigem exclusividade
    #     conn = self.session.connection().execution_options(isolation_level="AUTOCOMMIT")

    #     # deixe o SQLite esperar se algo ainda estiver liberando locks
    #     conn.exec_driver_sql("PRAGMA busy_timeout=60000")

    #     # se estivermos em WAL, faça checkpoint (limpa WAL e reduz chance de lock)
    #     try:
    #         conn.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
    #     except Exception as e:
    #         self.logger.log(f"⚠️ wal_checkpoint warning: {e}", "WARNING")

    #     # tente trocar o journal_mode com backoff
    #     for i in range(6):  # 0.5s, 1s, 2s, 4s, 8s, 16s
    #         try:
    #             mode = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
    #             if str(mode).upper() == "DELETE":
    #                 break
    #             newmode = conn.exec_driver_sql("PRAGMA journal_mode=DELETE").scalar()
    #             if str(newmode).upper() == "DELETE":
    #                 break
    #         except Exception as e:
    #             sleep = 0.5 * (2 ** i)
    #             self.logger.log(f"⏳ journal_mode switch blocked ({e}). Retrying in {sleep:.1f}s...", "WARNING")
    #             time.sleep(sleep)
    #             continue
    #         else:
    #             break

    #     # demais ajustes podem ser feitos sem exclusividade
    #     conn.exec_driver_sql("PRAGMA synchronous=FULL")
    #     conn.exec_driver_sql("PRAGMA locking_mode=NORMAL")
    #     conn.exec_driver_sql("PRAGMA foreign_keys=ON")

    def create_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Create indexes on the database to speed up queries.

        Parameters:
            index_specs: List of tuples (table_name, [column1, column2, ...])
        """
        engine = self.session.bind.dialect.name
        if engine not in ("sqlite", "postgresql"):
            self.logger.log(
                f"❌ Index creation not supported for engine: {engine}",
                "WARNING",  # noqa E501
            )  # noqa E501
            return

        for table, columns in index_specs:
            index_name = f"idx_{table}_{'_'.join(columns)}"
            col_str = ", ".join(columns)
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_str});"  # noqa E501
            self.session.execute(text(sql))
            self.logger.log(f"📌 Created index: {index_name}", "DEBUG")

        self.session.commit()

    def drop_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Drop indexes based on the provided table/column specs.

        Parameters:
            index_specs: List of tuples (table_name, [column1, column2, ...])
        """
        engine = self.session.bind.dialect.name
        if engine not in ("sqlite", "postgresql"):
            self.logger.log(
                f"❌ Index removal not supported for engine: {engine}",
                "WARNING",  # noqa E501
            )  # noqa E501
            return

        for table, columns in index_specs:
            index_name = f"idx_{table}_{'_'.join(columns)}"

            if engine == "sqlite":
                sql = f"DROP INDEX IF EXISTS {index_name};"
            elif engine == "postgresql":
                sql = f'DROP INDEX IF EXISTS "{index_name}";'

            self.session.execute(text(sql))

            self.logger.log(f"🗑️  Droped index: {index_name}", "DEBUG")  # noqa E501

        self.session.commit()

    @property
    def get_gene_index_specs(self):
        return [
            # GeneMaster indexes
            ("gene_masters", ["entity_id"]),
            ("gene_masters", ["symbol"]),
            ("gene_masters", ["locus_group_id"]),
            ("gene_masters", ["locus_type_id"]),
            ("gene_masters", ["data_source_id"]),
            ("gene_masters", ["omic_status_id"]),
            # GeneGroup
            ("gene_groups", ["name"]),
            ("gene_groups", ["data_source_id"]),
            # GeneLocusGroup
            ("gene_locus_groups", ["name"]),
            ("gene_locus_groups", ["data_source_id"]),
            # GeneLocusType
            ("gene_locus_types", ["name"]),
            ("gene_locus_types", ["data_source_id"]),
            # GeneGroupMembership
            ("gene_group_memberships", ["gene_id"]),
            ("gene_group_memberships", ["group_id"]),
            ("gene_group_memberships", ["data_source_id"]),
            # # GeneLocation
            # ("gene_locations", ["gene_id"]),
            # ("gene_locations", ["region_id"]),
            # ("gene_locations", ["assembly"]),
            # ("gene_locations", ["chromosome"]),
            # ("gene_locations", ["chromosome", "start_pos", "end_pos"]),
            # ("gene_locations", ["data_source_id"]),
            # # GeneGenomicRegion
            # ("gene_genomic_regions", ["label"]),
            # ("gene_genomic_regions", ["chromosome"]),
            # ("gene_genomic_regions", ["chromosome", "start_pos", "end_pos"]),
            # ("gene_genomic_regions", ["data_source_id"]),
        ]

    @property
    def get_protein_index_specs(self):
        return [
            # protein_master
            # ("protein_master", ["data_source_id", "protein_id"]),
            ("protein_masters", ["protein_id"]),
            # protein_entity
            ("protein_entities", ["entity_id"]),
            ("protein_entities", ["protein_id", "is_isoform"]),
            # ("protein_entity", ["data_source_id", "entity_id"]),
            # protein_pfam_link
            ("protein_pfam_links", ["pfam_pk_id"]),
            # ("protein_pfam_link", ["data_source_id"]),
        ]

    @property
    def get_entity_index_specs(self):
        return [
            # Entities
            ("entities", ["group_id"]),
            ("entities", ["has_conflict"]),
            ("entities", ["is_active"]),
            ("entities", ["data_source_id"]),
            # EntityAlias
            ("entity_aliases", ["entity_id"]),
            ("entity_aliases", ["alias_value"]),
            ("entity_aliases", ["alias_type"]),
            ("entity_aliases", ["xref_source"]),
            ("entity_aliases", ["alias_norm"]),
            ("entity_aliases", ["data_source_id"]),
            ("entity_aliases", ["entity_id", "is_primary"]),
            ("entity_aliases", ["xref_source", "alias_value"]),
            ("entity_aliases", ["data_source_id", "alias_value"]),
            # EntityRelationship
            ("entity_relationships", ["entity_1_id"]),
            ("entity_relationships", ["entity_2_id"]),
            ("entity_relationships", ["relationship_type_id"]),
            ("entity_relationships", ["data_source_id"]),
            ("entity_relationships", ["entity_1_id", "relationship_type_id"]),
            (
                "entity_relationships",
                ["entity_1_id", "entity_2_id", "relationship_type_id"],
            ),
            # EntityRelationshipType
            ("entity_relationship_types", ["code"]),
        ]

    # Only EntityRelationship Model
    @property
    def get_entity_relationship_index_specs(self):
        return [
            # EntityRelationship
            ("entity_relationships", ["entity_1_id"]),
            ("entity_relationships", ["entity_2_id"]),
            ("entity_relationships", ["relationship_type_id"]),
            ("entity_relationships", ["data_source_id"]),
            ("entity_relationships", ["entity_1_id", "relationship_type_id"]),
            (
                "entity_relationships",
                ["entity_1_id", "entity_2_id", "relationship_type_id"],
            ),
            # EntityRelationshipType
            ("entity_relationship_types", ["code"]),
        ]

    @property
    def get_entity_location_index_specs(self):
        """
        Index specs for the EntityLocation model.

        These are consumed by the ETL index manager to create/drop
        indexes around heavy loads.
        """
        return [
            # Core lookups
            ("entity_locations", ["entity_id"]),
            ("entity_locations", ["assembly_id"]),
            ("entity_locations", ["chromosome"]),
            ("entity_locations", ["build"]),
            # Region-style queries: "give me everything in chr N for this assembly"
            ("entity_locations", ["assembly_id", "chromosome"]),
            ("entity_locations", ["assembly_id", "chromosome", "start_pos"]),
            (
                "entity_locations",
                ["assembly_id", "chromosome", "start_pos", "end_pos"],
            ),
            # Fast uniqueness / existence check (matches the UniqueConstraint)
            ("entity_locations", ["entity_id", "assembly_id"]),
            # ETL housekeeping
            ("entity_locations", ["data_source_id"]),
            ("entity_locations", ["etl_package_id"]),
            # Optional: if you foresee queries like "all genes in region '12p13.31'"
            # ("entity_locations", ["region_label"]),
        ]

    @property
    def get_go_index_specs(self):
        return [
            # GOMaster
            ("go_masters", ["go_id"]),
            ("go_masters", ["entity_id"]),
            ("go_masters", ["namespace"]),
            # GORelation
            ("go_relations", ["parent_id"]),  # relações ascendentes
            ("go_relations", ["child_id"]),  # relações descendentes
            ("go_relations", ["relation_type"]),  # ex: is_a, part_of
            ("go_relations", ["parent_id", "relation_type"]),
            ("go_relations", ["child_id", "relation_type"]),
        ]

    @property
    def get_pathway_index_specs(self):
        return [
            ("pathway_masters", ["entity_id"]),
            ("pathway_masters", ["pathway_id"]),
            ("pathway_masters", ["data_source_id"]),
        ]

    @property
    def get_snp_index_specs(self):
        """
        Return a list of (table_name, [column_names]) to be used when
        creating indexes for SNP-related tables.

        This is used by the DB bootstrap / migration helper to create
        indexes in both SQLite and PostgreSQL.
        """
        return [
            # --- SNP main table ---
            # PK (rs_id) is already indexed by default, but we keep it
            # here for explicitness and for helper symmetry.
            ("variant_snps", ["rs_id"]),  # natural primary key
            # Common query patterns: by chromosome and position in each build
            ("variant_snps", ["chromosome"]),
            ("variant_snps", ["position_37"]),
            ("variant_snps", ["position_38"]),
            ("variant_snps", ["chromosome", "position_37"]),
            ("variant_snps", ["chromosome", "position_38"]),
            # Provenance filters (ETL / source system scoping)
            ("variant_snps", ["data_source_id"]),
            ("variant_snps", ["etl_package_id"]),
            # --- SNP merge table ---
            # Composite primary key: (rs_obsolete_id, rs_canonical_id)
            # PK also creates an index, but we expose them individually as well
            # for common lookup patterns.
            ("variant_snp_merges", ["rs_obsolete_id"]),
            ("variant_snp_merges", ["rs_canonical_id"]),
            # Provenance for merges
            ("variant_snp_merges", ["data_source_id"]),
            ("variant_snp_merges", ["etl_package_id"]),
            # (Optional) explicit composite index (even though PK already exists). # noqa E501
            ("variant_snp_merges", ["rs_obsolete_id", "rs_canonical_id"]),
        ]

    @property
    def get_variant_gwas_index_specs(self):
        return [
            # VariantGWAS
            ("variant_gwas", ["pubmed_id"]),
            ("variant_gwas", ["snp_id"]),
            ("variant_gwas", ["mapped_trait_id"]),
            ("variant_gwas", ["chr_id", "chr_pos"]),
        ]

    @property
    def get_disease_index_specs(self):
        return [
            # DiseaseMaster
            ("disease_masters", ["mondo_id"]),
            ("disease_masters", ["entity_id"]),
            ("disease_masters", ["data_source_id"]),
            # DiseaseGroupMembership
            ("disease_group_memberships", ["disease_id"]),
            ("disease_group_memberships", ["group_id"]),
            ("disease_group_memberships", ["data_source_id"]),
        ]

    @property
    def get_chemical_index_specs(self):
        return [
            # chemical_master
            ("chemical_masters", ["chemical_id"]),
            ("chemical_masters", ["entity_id"]),
            # ("chemical_data", ["chemical_id"]),
        ]
