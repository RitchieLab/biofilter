# biofilter/etl/base/conflict_resolution_mixin.py
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
    ConflictResolution,
)
from biofilter.db.models.entity_models import Entity, EntityName
from biofilter.db.models.omics_models import Gene
from sqlalchemy.exc import IntegrityError


class ConflictResolutionMixin:

    def is_conflict_resolved(self, identifier_type: str, identifier: str) -> bool:          # noqa E501
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

    def apply_resolution(self, row):
        """
        Applies a curation resolution rule to a gene with previously
        resolved conflict.

        Parameters:
        - row: the current row being processed (pandas Series)
        """
        hgnc_id = row.get("hgnc_id")

        # Search for the resolved conflict associated with this gene
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
