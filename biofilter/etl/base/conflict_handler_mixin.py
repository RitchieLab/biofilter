from sqlalchemy import or_
from typing import Optional
from biofilter.db.models.omics_models import Gene
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
)  # noqa: E501
from biofilter.db.models.entity_models import Entity


class ConflictHandlerMixin:

    def normalize_gene_identifiers(self, hgnc_id, entrez_id, ensembl_id):
        def clean_id(val):
            val = str(val).strip().upper() if val else None
            return val if val and val != "NAN" else None

        return (
            clean_id(hgnc_id),
            clean_id(entrez_id),
            clean_id(ensembl_id),
        )

    def detect_gene_conflict(
        self,
        hgnc_id: str,
        entrez_id: str,
        ensembl_id: str,
        entity_id: int,
        symbol: str,
    ) -> Optional[Gene]:
        """
        Returns existing Gene if safe, or logs a conflict and returns None.
        """
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
        if not existing_gene:
            return None

        # Same Gene!
        if (
            existing_gene.hgnc_id == hgnc_id
            and existing_gene.entrez_id == entrez_id
            and existing_gene.ensembl_id == ensembl_id
            and existing_gene.entity_id == entity_id
        ):
            msg = f"♻️ Gene already exists (identical): {symbol}"
            self.logger.log(msg, "DEBUG")
            return existing_gene

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

        if not conflicts:
            self.logger.log(f"♻️ Gene already exists: {symbol}", "INFO")
            return existing_gene

        # Log conflict
        description = (
            f"Gene {hgnc_id} conflicts with existing gene {existing_gene.hgnc_id}, "    # noqa: E501
            f"both share same identifier(s): {', '.join(conflicts)}"
        )

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
                description=description,
                entity_id=entity_id,
            )
            self.session.add(conflict)

        entity = self.session.query(Entity).filter_by(id=entity_id).first()
        if entity:
            entity.has_conflict = 1

        self.session.commit()
        self.logger.log(
            f"🚫 Conflict detected for Gene '{symbol}' - submitted for curation",       # noqa E501
            "WARNING",
        )
        return "CONFLICT"
