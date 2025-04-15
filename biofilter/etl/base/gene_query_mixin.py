import re
import ast
import pandas as pd
from sqlalchemy import or_
from biofilter.db.models.omics_models import (
    Gene,
    GeneGroup,
    GeneGroupMembership,
    LocusGroup,
    LocusType,
    GenomicRegion,
    GeneLocation,
)  # noqa: E501
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
    ConflictResolution,
)  # noqa: E501
from biofilter.db.models.entity_models import Entity, EntityName


class GeneQueryMixin:

    def is_conflict_resolved(self, identifier_type: str, identifier: str) -> bool:
        return (
            self.session.query(CurationConflict)
            .filter_by(
                entity_type="gene",
                identifier_type=identifier_type,
                identifier_value=identifier,
                status=ConflictStatus.resolved,
            )
            .first()
            is not None
        )

    def get_or_create_locus_group(self, name: str):
        """
        Retrieves an existing LocusGroup by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_group' field.

        Returns:
            LocusGroup or None
        """
        if not name or not isinstance(name, str):
            return None

        name_clean = name.strip()
        if not name_clean:
            return None

        group = (
            self.session.query(LocusGroup).filter_by(name=name_clean).first()
        )  # noqa: E501
        if group:
            return group

        # Create new LocusGroup
        locus_group = LocusGroup(name=name_clean)
        self.session.add(locus_group)
        self.session.flush()  # commits later in batch
        msg = f"LocusGroup '{name_clean}' created"
        self.logger.log(msg, "DEBUG")
        return locus_group

    def get_or_create_locus_type(self, name: str):
        """
        Retrieves an existing LocusType by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_type' field.

        Returns:
            LocusType or None
        """
        if not name or not isinstance(name, str):
            return None

        name_clean = name.strip()
        if not name_clean:
            return None

        locus_type = (
            self.session.query(LocusType).filter_by(name=name_clean).first()
        )  # noqa: E501
        if locus_type:
            return locus_type

        # Create new LocusType
        locus_type = LocusType(name=name_clean)
        self.session.add(locus_type)
        self.session.flush()  # commits later in batch
        self.logger.log(f"Created new LocusType: {name_clean}", "DEBUG")
        return locus_type

    def get_or_create_genomic_region(
        self,
        label: str,
        chromosome: str = None,
        start: int = None,
        end: int = None,
        description: str = None,
    ):
        """
        Returns an existing GenomicRegion by label, or creates a new one.
        """
        if not label or not isinstance(label, str):
            return None

        label_clean = label.strip()
        if not label_clean:
            return None

        region = (
            self.session.query(GenomicRegion).filter_by(label=label_clean).first()
        )  # noqa: E501
        if region:
            return region

        region = GenomicRegion(
            label=label_clean,
            chromosome=chromosome,
            start=start,
            end=end,
            description=description,
        )
        self.session.add(region)
        self.session.flush()
        msg = f"GenomicRegion '{label_clean}' created"
        self.logger.log(msg, "DEBUG")
        return region

    def get_or_create_gene_location(self):
        pass

    def get_or_create_gene(
        self,
        symbol: str,
        hgnc_status: str = None,
        hgnc_id: str = None,
        entrez_id: str = None,
        ensembl_id: str = None,
        entity_id: int = None,
        data_source_id: int = None,
        locus_group=None,
        locus_type=None,
        gene_group_names: list = None,
    ):
        """
        Creates or retrieves a gene based on unique identifiers (hgnc_id,
        entrez_id or entity_id). Also manages linking with GeneGroup and
        Memberships.
        """
        if not symbol:
            msg = f"⚠️ Gene {hgnc_id} ignored: empty symbol"
            self.logger.log(msg, "WARNING")
            return None

        # # Normalize data
        # entrez_id = str(entrez_id).strip().upper() if entrez_id else None
        # hgnc_id = str(hgnc_id).strip().upper() if hgnc_id else None
        # ensembl_id = str(ensembl_id).strip().upper() if ensembl_id else None

        # existing_gene = (
        #     self.session.query(Gene)
        #     .filter(
        #         or_(
        #             Gene.hgnc_id == hgnc_id,
        #             Gene.entrez_id == entrez_id,
        #             Gene.ensembl_id == ensembl_id,
        #             Gene.entity_id == entity_id,
        #         )
        #     )
        #     .first()
        # )

        # Limpeza dos campos
        def clean_id(val):
            val = str(val).strip().upper() if val else None
            return val if val and val != "NAN" else None

        entrez_id = clean_id(entrez_id)
        ensembl_id = clean_id(ensembl_id)
        hgnc_id = clean_id(hgnc_id)

        filters = []

        if hgnc_id:
            filters.append(Gene.hgnc_id == hgnc_id)
        if entrez_id:
            filters.append(Gene.entrez_id == entrez_id)
        if ensembl_id:
            filters.append(Gene.ensembl_id == ensembl_id)
        if entity_id:
            filters.append(Gene.entity_id == entity_id)

        existing_gene = self.session.query(Gene).filter(or_(*filters)).first()

        if existing_gene:
            conflicts = []

            if (
                entrez_id
                and existing_gene.entrez_id == entrez_id
                and existing_gene.hgnc_id != hgnc_id
            ):
                conflicts.append(f"entrez_id={entrez_id}")

            if (
                ensembl_id
                and existing_gene.ensembl_id == ensembl_id
                and existing_gene.hgnc_id != hgnc_id
            ):
                conflicts.append(f"ensembl_id={ensembl_id}")

            if conflicts:
                # Cria descrição detalhada
                conflict_description = (
                    f"Gene {hgnc_id} conflicts with existing gene {existing_gene.hgnc_id}, "
                    f"both share same identifier(s): {', '.join(conflicts)}"
                )

                # Verifica se já existe um conflito registrado com essa combinação
                already_logged = (
                    self.session.query(CurationConflict)
                    .filter_by(
                        entity_type="gene",
                        identifier=hgnc_id,
                        existing_identifier=existing_gene.hgnc_id,
                        status=ConflictStatus.pending,
                    )
                    .first()
                )

                if not already_logged:
                    conflict = CurationConflict(
                        entity_type="gene",
                        identifier=hgnc_id,
                        existing_identifier=existing_gene.hgnc_id,
                        status=ConflictStatus.pending,
                        description=conflict_description,
                        entity_id=entity_id,
                    )
                    self.session.add(conflict)

                # Marca a entidade como com conflito
                entity = self.session.query(Entity).filter_by(id=entity_id).first()
                if entity:
                    entity.has_conflict = 1

                self.session.commit()
                msg = (
                    f"🚫 Conflict detected for Gene '{symbol}' - submitted for curation"
                )
                self.logger.log(msg, "WARNING")
                return None

            else:
                msg = f"♻️ Gene already exists: {symbol}"
                self.logger.log(msg, "INFO")
                return existing_gene

        else:
            gene = Gene(
                # symbol=symbol,
                hgnc_status=hgnc_status,
                entity_id=entity_id,
                hgnc_id=hgnc_id,
                entrez_id=entrez_id,
                ensembl_id=ensembl_id,
                data_source_id=data_source_id,
                locus_group=locus_group,
                locus_type=locus_type,
            )
            self.session.add(gene)
            self.session.flush()
            msg = f"🧬 New Gene '{symbol}' created"
            self.logger.log(msg, "INFO")

        # Association with GeneGroup
        group_objs = []
        if gene_group_names:
            for group_name in gene_group_names:
                if not group_name:
                    continue
                group = (
                    self.session.query(GeneGroup)
                    .filter_by(name=group_name.strip())
                    .first()
                )
                if not group:
                    group = GeneGroup(name=group_name.strip())
                    self.session.add(group)
                    self.session.flush()
                    msg = f"🧩 GeneGroup '{group_name}' created"
                    self.logger.log(msg, "DEBUG")
                group_objs.append(group)

        # Vincula Gene aos grupos (GeneGroupMembership)
        existing_links = {
            g.group_id
            for g in self.session.query(GeneGroupMembership).filter_by(gene_id=gene.id)
        }

        new_links = 0
        for group in group_objs:
            if group.id not in existing_links:
                membership = GeneGroupMembership(gene_id=gene.id, group_id=group.id)
                self.session.add(membership)
                new_links += 1

        self.session.commit()
        self.logger.log(
            f"✅ Gene '{symbol}' salvo com {len(group_objs)} grupo(s), {new_links} vínculos adicionados",
            "INFO",
        )

        return gene

    def create_gene_location(
        self,
        gene: Gene,
        chromosome: str = None,
        start: int = None,
        end: int = None,
        strand: str = None,
        region: GenomicRegion = None,
        assembly: str = "GRCh38",
        data_source_id: int = None,
    ):
        """
        Cria uma entrada de localização para o Gene associado.

        Args:
            gene (Gene): Objeto Gene já existente.
            chromosome (str): Cromossomo (ex: "12").
            start (int): Posição inicial.
            end (int): Posição final.
            strand (str): Fita ("+" ou "-").
            region (GenomicRegion): Instância opcional da região genômica.
            assembly (str): Versão do genoma. Default = GRCh38.
            data_source_id (int): ID da fonte de dados.

        Returns:
            GeneLocation: Instância criada.
        """
        if not gene:
            msg = "⚠️ Gene Location invalid: Gene not provided"
            self.logger.log(msg, "WARNING")
            return None

        location = GeneLocation(
            gene_id=gene.id,
            chromosome=chromosome,
            start=start,
            end=end,
            strand=strand,
            region_id=region.id if region else None,
            assembly=assembly,
            data_source_id=data_source_id,
        )

        self.session.add(location)
        self.session.commit()

        self.logger.log(
            f"📌 GeneLocation criada para Gene '{gene.id}' no cromossomo {chromosome}",
            "DEBUG",
        )

        return location

    def parse_gene_groups(self, group_data) -> list:
        """
        Normaliza o campo gene_group para uma lista de strings.

        Args:
            group_data: Pode ser uma string (lista em texto ou valor único), lista real, ou None.

        Returns:
            Lista de nomes de grupos.
        """
        if pd.isna(group_data) or not group_data:
            return []

        if isinstance(group_data, str):
            try:
                parsed = ast.literal_eval(group_data)
                return parsed if isinstance(parsed, list) else [parsed]
            except (ValueError, SyntaxError):
                return [group_data.strip()]

        if isinstance(group_data, list):
            return [g.strip() for g in group_data if g]

        return [str(group_data).strip()]

    def extract_chromosome(self, location_sortable):
        if not location_sortable or pd.isna(location_sortable):
            return None

        match = re.match(r"^([0-9XYMT]+)", str(location_sortable).upper())
        if match:
            return match.group(1)
        return None

    def apply_resolution(self, row):
        """
        Applies a curation resolution rule to a gene with previously resolved conflict.

        Parameters:
        - row: the current row being processed (pandas Series)
        """
        hgnc_id = row.get("hgnc_id")
        symbol = row.get("symbol")  # NOTE: 'symbol' is not used in the function

        # Busca o conflito resolvido associado a esse gene
        conflict = (
            self.session.query(CurationConflict)
            .filter_by(
                entity_type="gene", identifier=hgnc_id, status=ConflictStatus.resolved
            )
            .first()
        )

        if not conflict:
            self.logger.log(f"❌ No resolved conflict found for {hgnc_id}", "ERROR")
            return False

        resolution = conflict.resolution

        # DELETE BLOCO
        if resolution == ConflictResolution.delete:
            self.logger.log(f"🗑️ Applying DELETE resolution for Gene {hgnc_id}", "INFO")

            # 🔒 Marca a Entity como desativada e com conflito
            entity = self.session.query(Entity).filter_by(id=conflict.entity_id).first()
            if entity:
                entity.has_conflict = True
                entity.is_deactive = True
                self.logger.log(
                    f"🔒 Entity {entity.id} marked as inactive due to conflict resolution (delete)",
                    "DEBUG",
                )
            else:
                self.logger.log(
                    f"⚠️ Entity ID {conflict.entity_id} not found. Cannot mark as inactive.",
                    "WARNING",
                )

            # ❌ Remove o Gene associado ao hgnc_id
            gene = self.session.query(Gene).filter_by(hgnc_id=hgnc_id).first()
            if gene:
                self.session.delete(gene)
                self.logger.log(f"✅ Gene {hgnc_id} deleted from database", "INFO")
            else:
                self.logger.log(
                    f"⚠️ Gene {hgnc_id} not found during delete resolution", "WARNING"
                )

            self.session.commit()
            return False

            """
            IMPORTANTE: A gente desativou a Entity e deletou o Gene caso esse exista,
            porem os aliases permaneceram apontando para a entity dessativada!!!!!
            """

        # MERGE BLOCO
        elif resolution == ConflictResolution.merge:
            """
            A resolução do tipo MERGE envolve:
            1. Desativar a entidade antiga (source_entity)
            2. Migrar todos os aliases (EntityName) da source_entity para a target_entity
            3. Marcar o Gene da source_entity como "merged"
            """

            self.logger.log(
                f"🔀 Applying MERGE resolution: {hgnc_id} → {conflict.existing_identifier}",
                "INFO",
            )

            # 1. Carrega Gene destino
            target_gene = (
                self.session.query(Gene)
                .filter_by(hgnc_id=conflict.existing_identifier)
                .first()
            )
            if not target_gene:
                self.logger.log(
                    f"❌ Target gene '{conflict.existing_identifier}' not found for merge",
                    "ERROR",
                )
                return False

            # 2. Marca a Entity antiga como inativa
            source_entity = (
                self.session.query(Entity).filter_by(id=conflict.entity_id).first()
            )
            if source_entity:
                source_entity.has_conflict = True
                source_entity.is_deactive = True
                self.logger.log(
                    f"🔒 Entity {source_entity.id} marked as inactive (merged)", "DEBUG"
                )
            else:
                self.logger.log(
                    f"⚠️ Source entity ID {conflict.entity_id} not found", "WARNING"
                )

            # 3. Migrar os EntityNames da entidade antiga
            # NOTE: Estamos mantendo o Codigo antigo como is_primary, resultando em dois nomes
            # primeiros para a mesma entity (isso é intencional por encanto
            migrated = 0
            for name_obj in (
                self.session.query(EntityName)
                .filter_by(entity_id=source_entity.id)
                .all()
            ):
                exists = (
                    self.session.query(EntityName)
                    .filter_by(entity_id=target_gene.entity_id, name=name_obj.name)
                    .first()
                )
                if exists:
                    self.session.delete(name_obj)
                else:
                    name_obj.entity_id = target_gene.entity_id
                    migrated += 1

            self.logger.log(
                f"🔁 Migrated {migrated} aliases to Entity {target_gene.entity_id}",
                "DEBUG",
            )

            # 4. Marcar Gene antigo como "merged"
            # NOTE: Nao exite o Gene antigo, uma vez que nao criamos eles no banco! Pensar se vamos ter esse omic??
            source_gene = self.session.query(Gene).filter_by(hgnc_id=hgnc_id).first()
            if source_gene:
                source_gene.hgnc_status = (
                    "merged"  # ou um campo próprio, como merged_into
                )
                # source_gene.merged_into = target_gene.id
                self.logger.log(f"📎 Gene '{hgnc_id}' marked as merged", "DEBUG")

            self.session.commit()
            return False

        # KEEP_BOTH BLOCO
        elif resolution == ConflictResolution.keep_both:
            """
            A resolução do tipo KEEP_BOTH mantém os dois genes no sistema, mesmo com conflito em IDs.
            A entidade dominante (item_exist) será usada em casos ambíguos para relacionamentos e anotações.
            """

            self.logger.log(
                f"⚖️ Applying KEEP_BOTH resolution: {hgnc_id} and {conflict.item_exist}",
                "INFO",
            )

            # 1. Marca a Entity como tendo conflito (mas não desativa!)
            entity = self.session.query(Entity).filter_by(id=conflict.entity_id).first()
            if entity:
                entity.has_conflict = True
                self.logger.log(
                    f"⚠️ Entity {entity.id} marked with conflict (keep_both)", "DEBUG"
                )
            else:
                self.logger.log(
                    f"⚠️ Entity not found for ID {conflict.entity_id}", "WARNING"
                )

            # 2. Log para rastreabilidade
            self.logger.log(
                f"✔️ Both genes '{hgnc_id}' and '{conflict.item_exist}' kept — shared ID resolution will favor '{conflict.item_exist}'",
                "INFO",
            )

            # TODO: Precisamos criar o Gene aqui.

            # 3. Nenhuma alteração estrutural adicional é necessária — o sistema de resolução (em consultas downstream)
            # precisará estar ciente das regras de dominância, usando o campo `item_exist` como referência.

            self.session.commit()

            return False  # Não processar mais esse gene
