import re
import ast
import pandas as pd
from biofilter.db.models.variants_models import (
    VariantType,
    AlleleType,
    GenomeAssembly,
    Variant,
    VariantLocation,
    GeneVariantLink,
)


class VariantQueryMixin:

    def get_or_create_variant_type(self, name):
        if name not in self._variant_type_cache:
            obj = self.session.query(VariantType).filter_by(name=name).first()
            if not obj:
                obj = VariantType(name=name)
                self.session.add(obj)
                self.session.flush()
            self._variant_type_cache[name] = obj
        return self._variant_type_cache[name]

    def get_or_create_allele_type(self, name):
        if name not in self._allele_type_cache:
            obj = self.session.query(AlleleType).filter_by(name=name).first()
            if not obj:
                obj = AlleleType(name=name)
                self.session.add(obj)
                self.session.flush()
            self._allele_type_cache[name] = obj
        return self._allele_type_cache[name]

    def get_or_create_assembly(self, accession):
        if accession not in self._assembly_cache:
            obj = self.session.query(GenomeAssembly).filter_by(accession=accession).first()
            if not obj:
                raise ValueError(f"GenomeAssembly not found for accession: {accession}")
            self._assembly_cache[accession] = obj
        return self._assembly_cache[accession]

    def get_gene_by_entrez(self, entrez_id):
        return self.session.query(Gene).filter_by(entrez_id=str(entrez_id)).first()

    # def get_or_create_variant(self, row, datasource_id):
    def get_or_create_variant(self, row, data_source_id):
        rs_id = row["rs_id"]
        build_id = int(row["build_id"])
        seq_id = int(row["assembly_id"])
        var_type = int(row["variant_type_id"])
        data_source = data_source_id
        # allele_type = row["allele_type"]
        # hgvs = row["hgvs"]
        # allele = row["allele"]

        # position_base_1 = int(row["position_base_1"])
        # position_start = int(row["position_start"])
        # position_end = int(row["position_end"])

        # gene_ids = eval(row["gene_ids"]) if isinstance(row["gene_ids"], str) else row["gene_ids"]

        # variant_type_obj = self.get_or_create_variant_type(var_type)
        # allele_type_obj = self.get_or_create_allele_type(allele_type)
        # assembly_obj = self.get_or_create_assembly(seq_id)

        # Search for existing variant
        variant = (
            self.session.query(Variant)
            .filter_by(
                external_id=rs_id,
                assembly_id=seq_id,
                build_id=build_id,
                variant_type_id=var_type,
            )
            .first()
        )

        if not variant:
            variant = Variant(
                external_id=rs_id,
                # entity_id=-1,  # 🚧 TODO: integrar com sistema de entidades
                variant_type_id=var_type,
                assembly_id=seq_id,
                data_source_id=data_source,
                build_id=build_id,
            )
            self.session.add(variant)
            self.session.flush()

        # # Criar localização
        # location = VariantLocation(
        #     variant_id=variant.id,
        #     assembly_id=assembly_obj.id,
        #     hgvs=hgvs,
        #     position_base_1=position_base_1,
        #     position_start=position_start,
        #     position_end=position_end,
        #     allele_type_id=allele_type_obj.id,
        #     allele=allele,
        # )
        # self.session.add(location)

        # # Criar links com genes
        # for entrez in gene_ids or []:
        #     gene = self.get_gene_by_entrez(entrez)
        #     if gene:
        #         exists = (
        #             self.session.query(GeneVariantLink)
        #             .filter_by(gene_id=gene.id, variant_id=variant.id)
        #             .first()
        #         )
        #         if not exists:
        #             self.session.add(GeneVariantLink(gene_id=gene.id, variant_id=variant.id))

        return variant
