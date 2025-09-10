from sqlalchemy import text


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

    def create_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Create indexes on the database to speed up queries.

        Parameters:
            index_specs: List of tuples (table_name, [column1, column2, ...])
        """
        engine = self.session.bind.dialect.name
        if engine not in ("sqlite", "postgresql"):
            self.logger.log(
                f"❌ Index creation not supported for engine: {engine}", "WARNING"
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
                f"❌ Index removal not supported for engine: {engine}", "WARNING"
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
            # GeneLocation
            ("gene_locations", ["gene_id"]),
            ("gene_locations", ["region_id"]),
            ("gene_locations", ["assembly"]),
            ("gene_locations", ["chromosome"]),
            ("gene_locations", ["chromosome", "start", "end"]),
            ("gene_locations", ["data_source_id"]),
            # GeneGenomicRegion
            ("gene_genomic_regions", ["label"]),
            ("gene_genomic_regions", ["chromosome"]),
            ("gene_genomic_regions", ["chromosome", "start", "end"]),
            ("gene_genomic_regions", ["data_source_id"]),
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

    # @property
    # def get_gene_index_specs(self):
    #     return [
    #         # ──────────────── gene_groups ────────────────
    #         ("gene_groups", ["name"]),
    #         # ──────────────── locus_groups ────────────────
    #         ("gene_locus_groups", ["name"]),
    #         # ──────────────── locus_types ────────────────
    #         ("gene_locus_types", ["name"]),
    #         # ──────────────── Gene Symbol ────────────────
    #         ("gene_symbol", ["symbol"]),
    #         # ──────────────── genomic_regions ────────────────
    #         # ("gene_genomic_regions", ["label"]),
    #         # ("gene_genomic_regions", ["chromosome"]),
    #         # ("gene_genomic_regions", ["chromosome", "start", "end"]),
    #         # ──────────────── genes ────────────────
    #         ("gene_masters", ["entity_id"]),
    #         ("gene_masters", ["symbol"]),
    #         # ("gene_masters", ["locus_group_id"]),
    #         # ("gene_masters", ["locus_type_id"]),
    #         # ("gene_masters", ["data_source_id"]),
    #         # ("gene_masters", ["omic_status_id"]),
    #         # ──────────────── gene_group_membership ────────────────
    #         ("gene_group_memberships", ["group_id"]),
    #         ("gene_group_memberships", ["gene_id"]),
    #         # # ──────────────── gene_locations ────────────────
    #         # ("gene_locations", ["gene_id"]),
    #         # ("gene_locations", ["region_id"]),
    #         # ("gene_locations", ["assembly"]),
    #         # ("gene_locations", ["chromosome"]),
    #         # ("gene_locations", ["chromosome", "start", "end"]),
    #         # ("gene_locations", ["data_source_id"]),
    #         # # ...
    #     ]

    # @property
    # def get_entity_index_specs(self):

    #     return [
    #         # Entity
    #         ("entities", ["group_id"]),
    #         ("entities", ["has_conflict"]),
    #         ("entities", ["is_deactive"]),
    #         # EntityName
    #         ("entity_names", ["entity_id"]),
    #         ("entity_names", ["name"]),
    #         ("entity_names", ["data_source_id"]),
    #         ("entity_names", ["data_source_id", "name"]),
    #         ("entity_names", ["data_source_id", "entity_id"]),
    #         ("entity_names", ["entity_id", "is_primary"]),
    #         # EntityRelationship
    #         ("entity_relationships", ["entity_1_id"]),
    #         ("entity_relationships", ["entity_2_id"]),
    #         ("entity_relationships", ["relationship_type_id"]),
    #         ("entity_relationships", ["data_source_id"]),
    #         (
    #             "entity_relationships",
    #             ["entity_1_id", "relationship_type_id"],
    #         ),  # noqa E501
    #         (
    #             "entity_relationships",
    #             ["entity_1_id", "entity_2_id", "relationship_type_id"],
    #         ),  # noqa E501
    #         # EntityRelationshipType
    #         ("entity_relationship_types", ["code"]),
    #     ]
