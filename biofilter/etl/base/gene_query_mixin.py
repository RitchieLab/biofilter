import ast
import pandas as pd
from sqlalchemy import or_

# from sqlalchemy.exc import IntegrityError
import re

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
    ConflictStatus
)
from biofilter.db.models.entity_models import Entity


class GeneQueryMixin:

    def is_conflict_resolved(self, identifier_type: str, identifier: str) -> bool:
        return self.session.query(CurationConflict).filter_by(
            entity_type="gene",
            identifier_type=identifier_type,
            identifier_value=identifier,
            status=ConflictStatus.resolved
        ).first() is not None

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

        # Normalize data
        entrez_id = str(entrez_id).strip().upper() if entrez_id else None
        hgnc_id = str(hgnc_id).strip().upper() if hgnc_id else None
        ensembl_id = str(ensembl_id).strip().upper() if ensembl_id else None

        existing_gene = (
            self.session.query(Gene)
            .filter(
                or_(
                    Gene.hgnc_id == hgnc_id,
                    Gene.entrez_id == entrez_id,
                    Gene.ensembl_id == ensembl_id,
                    Gene.entity_id == entity_id,
                )
            )
            .first()
        )

        if existing_gene:
            conflicts = []

            if entrez_id and existing_gene.entrez_id == entrez_id and existing_gene.hgnc_id != hgnc_id:
                conflicts.append(f"entrez_id={entrez_id}")

            if ensembl_id and existing_gene.ensembl_id == ensembl_id and existing_gene.hgnc_id != hgnc_id:
                conflicts.append(f"ensembl_id={ensembl_id}")

            if conflicts:
                # Cria descrição detalhada
                conflict_description = (
                    f"Gene {hgnc_id} conflicts with existing gene {existing_gene.hgnc_id}, "
                    f"both share same identifier(s): {', '.join(conflicts)}"
                )

                # Verifica se já existe um conflito registrado com essa combinação
                already_logged = self.session.query(CurationConflict).filter_by(
                    entity_type="gene",
                    identifier=hgnc_id,
                    existing_identifier=existing_gene.hgnc_id,
                    status=ConflictStatus.pending
                ).first()

                if not already_logged:
                    conflict = CurationConflict(
                        entity_type="gene",
                        identifier=hgnc_id,
                        existing_identifier=existing_gene.hgnc_id,
                        status=ConflictStatus.pending,
                        description=conflict_description
                    )
                    self.session.add(conflict)

                # Marca a entidade como com conflito
                entity = self.session.query(Entity).filter_by(id=entity_id).first()
                if entity:
                    entity.has_conflict = 1

                self.session.commit()
                msg = f"🚫 Conflict detected for Gene '{symbol}' - submitted for curation"
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
        - resolved_conflicts: a dict mapping gene identifiers (e.g., HGNC ID) to their resolution info
        """
        hgnc_id = row.get("hgnc_id")

        # Busca o conflito resolvido associado a esse gene
        conflict = self.session.query(CurationConflict).filter_by(
            entity_type="gene",
            identifier=hgnc_id,
            status=ConflictStatus.resolved
        ).first()

        if not conflict:
            self.logger.log(f"❌ No resolved conflict found for {hgnc_id}", "ERROR")
            return False

        # Segue a lógica com base na resolução

        # 🚫 Resolução do tipo 'delete':
        # Caso a decisão de curação tenha sido excluir esse gene (por exemplo, por duplicidade ou obsolescência),
        # não aplicamos nenhum processamento adicional. O gene é intencionalmente ignorado e não será carregado
        # na base principal, mantendo a integridade conforme definido pelo curador.

        ==> Parei aqui. pensar em como sera cada uma das operacoes

        if conflict.resolution == conflict.delete:
            self.logger.log(f"🗑️ Gene {hgnc_id} will be skipped (deleted)", "INFO")
            return False

        elif conflict.resolution == conflict.merge:
            # Aqui você pode carregar o gene alvo (conflict.item_exist) e transferir os dados
            self.logger.log(f"🔀 Merging gene {hgnc_id} into {conflict.item_exist}", "INFO")
            # Exemplo: adicionar `row` como alias de outro Gene
            # ou transferir atributos
            # ✅ Use the resolved winner gene as the target for aliases and groups
            # winner_entity = (
            #     self.session.query(EntityName)
            #     .filter_by(name=resolved_with)
            #     .first()
            # )

            # if not winner_entity:
            #     self.logger.log(
            #         f"⚠️ Cannot merge: winner gene '{resolved_with}' not found", "WARNING"
            #     )
            #     return None

            # self.logger.log(
            #     f"🔀 Gene '{gene_id}' merged into '{resolved_with}'", "INFO"
            # )

            # # Adiciona aliases do gene atual ao vencedor
            # aliases = self.extract_aliases(row, drop=[gene_id])
            # for alias in aliases:
            #     self.add_entity_name(
            #         entity_id=winner_entity.entity_id,
            #         alt_name=alias,
            #         data_source_id=self.datasource.id,
            #     )

            # # Aqui poderíamos adicionar grupos, locations, etc. se apropriado

            # return "merged"  # ou retornar algo útil se quiser logar ou acumular

        elif conflict.resolution == conflict.keep_both:
            self.logger.log(f"✅ Keeping both records; cleaning conflicting field", "INFO")
            # Exemplo: apagar apenas o campo conflitante de `row`
        #     self.logger.log(
        #         f"👥 Keeping both versions of gene '{gene_id}' (resolved conflict)", "INFO"
        #     )

        #     # ⚠️ Deve remover valores conflitantes antes de criar o Gene
        #     for field in ["entrez_id", "ensembl_gene_id"]:
        #         resolved_field = getattr(resolution_entry, "identifier_type", None)
        #         if resolved_field == field:
        #             row[field] = None  # remove o valor conflitante

        #     return "keep_both"

        # self.logger.log(f"❓ Unknown resolution for {gene_id}: {resolution}", "WARNING")
        # return None
