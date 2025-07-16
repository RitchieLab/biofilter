import os
import re
import ast
import bz2
import json
import pandas as pd
# from pathlib import Path

# from typing import Optional
import __main__
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.mixins.variant_query_mixin import VariantQueryMixin

from biofilter.db.models.variants_models import Variant, VariantLocation, GeneVariantLink
from biofilter.db.models.omics_models import Gene


from biofilter.db.models.variants_models import (
    VariantType,
    AlleleType,
    GenomeAssembly,
    Variant,
    VariantLocation,
    GeneVariantLink,
)


def transform_batch(lines_batch):
    results = []

    for line in lines_batch:
        record = json.loads(line)
        rs_id = f"{record['refsnp_id']}"
        last_build_id = record.get("last_update_build_id", None)
        primary_data = record.get("primary_snapshot_data", {})
        variant_type = primary_data.get("variant_type", None)

        # Run only last build
        placements = primary_data.get("placements_with_allele", [])
        ptlp_placement = next(
            (p for p in placements if p.get("is_ptlp", False)), None
        )  # noqa: E501

        # Get Genes ID
        gene_ids = set()
        for allele_annot in primary_data.get("allele_annotations", []):
            for assembly in allele_annot.get("assembly_annotation", []):
                for gene in assembly.get("genes", []):
                    gene_id = gene.get("id")
                    if gene_id:
                        gene_ids.add(gene_id)

        # Get Allele Info
        if ptlp_placement:
            for allele_info in ptlp_placement.get("alleles", []):

                # Get Data
                hgvs = allele_info.get("hgvs")
                spdi = allele_info.get("allele", {}).get("spdi", {})
                seq_id = spdi.get("seq_id")
                spdi_position = spdi.get("position")
                position_base_1 = int(spdi_position + 1)
                alt_seq = spdi.get("inserted_sequence")

                match = re.match(r"^(.*?):g\.([\d_]+)(.*)$", hgvs)
                pos_raw = match.group(2)
                suffix = match.group(3)

                # Positions
                if "_" in pos_raw:
                    pos_start, pos_end = map(int, pos_raw.split("_"))
                else:
                    pos_start = pos_end = int(pos_raw)

                # Type
                if suffix == "=":
                    allele_type = "ref"
                elif "del" in suffix:
                    allele_type = "del"
                elif "dup" in suffix:
                    allele_type = "dup"
                elif re.search(r"\[\d+\]$", suffix):
                    allele_type = "rep"
                elif re.match(r"[ACGT]>[ACGT]", suffix):
                    allele_type = "sub"
                else:
                    allele_type = "oth"

                results.append({
                    "rs_id": rs_id,
                    "build_id": last_build_id,
                    "seq_id": seq_id,
                    "var_type": variant_type,
                    "hgvs": hgvs,
                    "position_base_1": position_base_1,
                    "position_start": pos_start,
                    "position_end": pos_end,
                    "allele_type": allele_type,
                    "allele": alt_seq,
                    "gene_ids": list(gene_ids),
                })

    return results


class DTP(DTPBase, VariantQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

    def extract(self, raw_dir: str, source_url: str, last_hash: str):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """

        try:
            message = ""

            # Landing path
            landing_path = os.path.join(
                raw_dir,
                self.datasource.source_system.name,
                self.datasource.name,
            )

            # Get hash from remote md5 file
            url_md5 = f"{source_url}.md5"
            remote_hash = self.get_md5_from_url_file(url_md5)
            if not remote_hash:
                msg = f"Failed to retrieve MD5 from {url_md5}"
                self.logger.log(msg, "WARNING")

            # Compare remote hash and last processed hash
            if remote_hash == last_hash:
                message = f"File already downloaded and hash matches: {last_hash}"  # noqa: E501
                self.logger.log(message, "INFO")
                return True, message, remote_hash

            # Download the file
            status, message = self.http_download(source_url, landing_path)

            if not status:
                self.logger.log(message, "ERROR")
                return False, message, remote_hash

            return True, message, remote_hash

        except Exception as e:
            message = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(message, "ERROR")
            return False, message, None

    def transform(self, raw_path, processed_path):

        # INPUT DATA
        input_file = self.get_raw_file(raw_path)
        if not input_file.exists():
            msg = f"❌ Input file not found: {input_file}."
            msg += " Consider running the extract() step or checking the source URL."  # noqa: E501
            self.logger.log(msg, "ERROR")
            return None, False, msg

        # OUTPUT DATA
        output_dir = self.get_path(processed_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # VARIABLES
        batch_size: int = 1000
        max_workers: int = 10
        # assembly: str = "GRCh38"
        status = False

        results = []
        futures = []
        batch = []

        try:
            with bz2.open(input_file, "rt", encoding="utf-8") as f:
                # if hasattr(__main__, "__file__"):
                if __name__ == "__main__" or (hasattr(__main__, "__file__") and not hasattr(sys, "ps1")):
                    with ProcessPoolExecutor(max_workers=max_workers) as executor:

                        for line in f:
                            batch.append(line)
                            if len(batch) >= batch_size:
                                futures.append(
                                    executor.submit(transform_batch, batch)
                                )  # noqa: E501
                                batch = []

                        # Runs the last batch
                        if batch:
                            futures.append(executor.submit(transform_batch, batch))

                        # Rescues the results from the futures
                        for future in as_completed(futures):
                            try:
                                batch_result = future.result()
                                results.extend(batch_result)
                            except Exception as e:
                                msg = f"⚠️ Worker failed during batch transform: {e}"
                                self.logger.log(msg, "WARNING")

                else:
                    msg = "⚠️ Skipping multiprocessing: not in __main__ context."
                    self.logger.log(msg, "WARNING")

            # Save the results to a CSV file
            transform_df = pd.DataFrame(results)

            # # Buscar os valores do banco
            # variant_type_map = {v.name: v.id for v in self.session.query(VariantType)}
            # allele_type_map = {a.name: a.id for a in self.session.query(AlleleType)}
            # assembly_map = {a.accession: a.id for a in self.session.query(GenomeAssembly)}

            # # Mapear as colunas
            # transform_df["variant_type_id"] = transform_df["var_type"].map(variant_type_map)
            # transform_df["allele_type_id"] = transform_df["allele_type"].map(allele_type_map)
            # transform_df["assembly_id"] = transform_df["seq_id"].map(assembly_map)

            variant_type_map = {v.name: str(v.id) for v in self.session.query(VariantType)}
            allele_type_map = {a.name: str(a.id) for a in self.session.query(AlleleType)}
            assembly_map = {a.accession: str(a.id) for a in self.session.query(GenomeAssembly)}

            transform_df["variant_type_id"] = transform_df["var_type"].map(variant_type_map)
            transform_df["allele_type_id"] = transform_df["allele_type"].map(allele_type_map)
            transform_df["assembly_id"] = transform_df["seq_id"].map(assembly_map)

            column_order = [
                "build_id",
                "rs_id",
                "seq_id",
                "assembly_id",
                "var_type",
                "variant_type_id",
                "hgvs",
                "position_base_1",
                "position_start",
                "position_end",
                "allele_type",
                "allele_type_id",
                "allele",
                "gene_ids",
            ]

            # Reorganiza o DataFrame (ignora colunas faltantes)
            transform_df = transform_df[[col for col in column_order if col in transform_df.columns]]

            transform_df.to_csv(output_dir / "processed_data.csv", index=False)

            msg = f"✅ Transform completed with {len(transform_df)} records."
            self.logger.log(msg, "INFO")
            status = True
            return transform_df, status, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # # 🚧 🚜 In developing
    # def load(self, df=None, processed_path=None):
    #     total_variant = 0  # not considered conflict genes
    #     load_status = False
    #     message = ""

    #     data_source_id = self.datasource.id

    #     # Models that will be used to store the data
    #     # - VariantType,
    #     # - AlleleType,
    #     # - GenomeAssembly,
    #     # - Variant,
    #     # - VariantLocation,
    #     # - GeneVariantLink,

    #     if df is None:
    #         if not processed_path:
    #             msg = "Either 'df' or 'processed_path' must be provided."
    #             self.logger.log(msg, "ERROR")
    #             return total_variant, load_status, message
    #             # raise ValueError(msg)
    #         msg = f"Loading data from {processed_path}"
    #         self.logger.log(msg, "INFO")

    #         # PROCESSED DATA
    #         processed_path = self.get_path(processed_path)
    #         processed_data = str(processed_path) + "/processed_data.csv"

    #         if not os.path.exists(processed_data):
    #             msg = f"File not found: {processed_data}"
    #             self.logger.log(msg, "ERROR")
    #             return total_variant, load_status, msg

    #         df = pd.read_csv(processed_data, dtype=str)

    #     variant_group_cols = ["build_id", "rs_id", "assembly_id", "variant_type_id"]

    #     # Agrupa e remove duplicatas (mantém apenas uma linha por variante)
    #     df_variants = df[variant_group_cols].drop_duplicates()

    #     # Preparar os dfs para cargas
    #     # 1. Criar um DS para carregar na Variants
    #     # 2. Criar um DS para carregar na Locations
    #     # 3. Criar um DS para carregar na Gene x Variant

    #     """"
    #     build_id,   rs_id,      seq_id,         assembly_id,    var_type,   variant_type_id,    hgvs,position_base_1,           position_start,position_end,allele_type,allele_type_id,allele,gene_ids
    #     157,        2086265909, NC_000024.10,   24,             ins,        7,                  NC_000024.10:g.1176407_1176408=,1176408,1176407,1176408,ref,1,,[]
    #     157,        2086265909, NC_000024.10,   24,             ins,        7,                  NC_000024.10:g.1176407_1176408insAATAATAATAATAGATTATAATTATAAAATAATAATAAAATATTATTATTAT,1176408,1176407,1176408,oth,7,AATAATAATAATAGATTATAATTATAAAATAATAATAAAATATTATTATTAT,[]

    #     Variants Model
    #     build_id           build_id
    #     rs_id              external_id
    #     seq_id             
    #     assembly_id        assembly_id
    #     var_type           
    #     variant_type_id    variant_type_id
    #     hgvs               object
    #     position_base_1    object
    #     position_start     object
    #     position_end       object
    #     allele_type        object
    #     allele_type_id     object
    #     allele             object
    #     gene_ids           object
    #     dtype: object
    #     """

    #     for _, row in df_variants.iterrows():
    #         status = self.get_or_create_variant(row, data_source_id)




    #             # o que fazer aqui:
    #             # Carregar a tabela de variant
    #             # Carregar a tabela de Locations
    #             # Carregar a gene x Variant

    #             # try:
    #             #     variant_mgr.get_or_create_variant(row, datasource_id=self.datasource.id)
    #             #     total_variant += 1
    #             # except Exception as e:
    #             #     self.logger.log(f"❌ Failed to load variant {row['rs_id']}: {str(e)}", "WARNING")

    #     return True

    def load(self, df=None, processed_path=None, chunk_size=100_000):
        total_variants = 0
        load_status = False
        message = ""

        # 🚨 Garante que self.datasource é válido na sessão atual
        self.datasource = self.session.merge(self.datasource)
        data_source_id = self.datasource.id

        if df is None:
            if not processed_path:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            processed_path = self.get_path(processed_path)
            processed_data = str(processed_path / "processed_data.csv")

            if not os.path.exists(processed_data):
                msg = f"File not found: {processed_data}"
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            self.logger.log(f"📥 Reading data in chunks from {processed_data}", "INFO")

            try:
                for chunk_df in pd.read_csv(processed_data, dtype=str, chunksize=chunk_size):
                    self.logger.log(f"⚙️ Processing chunk with {len(chunk_df)} rows...", "INFO")
                    n = self._process_chunk(chunk_df, data_source_id)
                    total_variants += n

                msg = f"✅ Loaded {total_variants} variant records successfully."
                self.logger.log(msg, "INFO")
                return total_variants, True, msg

            except Exception as e:
                msg = f"❌ ETL Load failed: {str(e)}"
                self.logger.log(msg, "ERROR")
                return total_variants, False, msg

        else:
            # 🔁 fallback para df em memória
            n = self._process_chunk(df, data_source_id)
            total_variants += n
            return total_variants, True, "Loaded from in-memory DataFrame."

    def _process_chunk(self, df_chunk, data_source_id):

        variant_cache = {}  # rs_id -> variant_id
        gene_cache = {}     # entrez_id -> gene_id
        gene_variant_seen = set()
        location_seen = set()

        variants_to_insert = []
        location_cache = set()
        gene_variant_cache = set()

        for _, row in df_chunk.iterrows():
            # rs_id = row["rs_id"]
            # build_id = int(row["build_id"])
            # assembly_id = int(row["assembly_id"])
            # variant_type_id = int(row["variant_type_id"])
            # allele_type_id = int(row["allele_type_id"]) if row["allele_type_id"] else None
            rs_id = row.get("rs_id")

            # Campos obrigatórios — necessário logar erro se faltarem
            # if pd.isna(rs_id) or pd.isna(row.get("assembly_id")) or pd.isna(row.get("variant_type_id")):
            #     self.logger.log(f"⚠️ Skipping row with missing critical fields: {row.to_dict()}", "WARNING")
            #     continue

            # Conversão segura com fallback para None
            # TODO: Precisamos de um padrão para os IDs?
            try:
                assembly_id = int(row["assembly_id"])
            except (ValueError, TypeError):
                assembly_id = 1

            try:
                variant_type_id = int(row["variant_type_id"])
            except (ValueError, TypeError):
                variant_type_id = 1

            try:
                allele_type_id = int(row["allele_type_id"]) if not pd.isna(row["allele_type_id"]) else None
            except (ValueError, TypeError):
                allele_type_id = 1

            try:
                build_id = int(row["build_id"]) if not pd.isna(row["build_id"]) else None
            except (ValueError, TypeError):
                build_id = 1


            # 🔄 Skip if already created in this chunk
            if rs_id not in variant_cache:

                # Check if the variant already exists in the database
                variant = (
                    self.session.query(Variant)
                    .filter_by(
                        external_id=rs_id,
                        assembly_id=assembly_id,
                        variant_type_id=variant_type_id,
                    )
                    .first()
                )

                if not variant:
                    variant = Variant(
                        external_id=rs_id,
                        # build_id=build_id,
                        assembly_id=assembly_id,
                        variant_type_id=variant_type_id,
                        data_source_id=data_source_id,
                        entity_id=1  # placeholder
                    )
                    self.session.add(variant)
                    self.session.flush()  # get variant.id

                variant_cache[rs_id] = variant.id

            variant_id = variant_cache[rs_id]

            # LOCATIONS
            # Key para deduplicar localizações no chunk
            location_key = (
                variant_id,
                row["hgvs"],
                int(row["position_base_1"]),
                int(row["position_start"]),
                int(row["position_end"]),
                allele_type_id,
                row["allele"]
            )

            if location_key not in location_cache:
                exists = self.session.query(VariantLocation).filter_by(
                    variant_id=variant_id,
                    hgvs=row["hgvs"],
                    # position_base_1=int(row["position_base_1"]),
                    # position_start=int(row["position_start"]),
                    # position_end=int(row["position_end"]),
                    # allele_type_id=allele_type_id,
                    # allele=row["allele"]
                ).first()

                if not exists:
                    loc = VariantLocation(
                        variant_id=variant_id,
                        assembly_id=assembly_id,
                        hgvs=row["hgvs"],
                        position_base_1=int(row["position_base_1"]),
                        position_start=int(row["position_start"]),
                        position_end=int(row["position_end"]),
                        allele_type_id=allele_type_id,
                        allele=row["allele"]
                    )
                    self.session.add(loc)
                    self.session.flush()

                location_cache.add(location_key)

            # GENE VARIANT LINK
            gene_ids = ast.literal_eval(row["gene_ids"]) if isinstance(row["gene_ids"], str) else row["gene_ids"]
            for entrez_id in gene_ids:
                if entrez_id not in gene_cache:
                    gene = self.session.query(Gene).filter_by(entrez_id=str(entrez_id)).first()
                    if gene:
                        gene_cache[entrez_id] = gene.id
                    else:
                        continue

                gene_id = gene_cache[entrez_id]
                link_key = (gene_id, variant_id)

                if link_key not in gene_variant_cache:
                    exists = self.session.query(GeneVariantLink).filter_by(
                        gene_id=gene_id,
                        variant_id=variant_id
                    ).first()

                    if not exists:
                        link = GeneVariantLink(gene_id=gene_id, variant_id=variant_id)
                        self.session.add(link)
                        self.session.flush()

                    gene_variant_cache.add(link_key)


        #     location_key = (
        #         variant_id,
        #         row["hgvs"],
        #         int(row["position_start"]),
        #         int(row["position_end"]),
        #         row["allele"],
        #     )

        #     if location_key not in location_seen:
        #         location_seen.add(location_key)

        #         # locations_to_insert.append(VariantLocation(
        #         #     variant_id=variant_id,
        #         #     assembly_id=assembly_id,
        #         #     hgvs=row["hgvs"],
        #         #     position_base_1=int(row["position_base_1"]),
        #         #     position_start=int(row["position_start"]),
        #         #     position_end=int(row["position_end"]),
        #         #     allele_type_id=allele_type_id,
        #         #     allele=row["allele"]
        #         # ))
        #         location = VariantLocation(
        #             variant_id=variant_id,
        #             assembly_id=assembly_id,
        #             hgvs=row["hgvs"],
        #             position_base_1=int(row["position_base_1"]),
        #             position_start=int(row["position_start"]),
        #             position_end=int(row["position_end"]),
        #             allele_type_id=allele_type_id,
        #             allele=row["allele"]
        #         )
        #         self.session.merge(location)


        #     # GENES
        #     # ✅ Process gene links (deduplicated)
        #     gene_ids = ast.literal_eval(row["gene_ids"]) if isinstance(row["gene_ids"], str) else row["gene_ids"]
        #     for entrez_id in gene_ids:
        #         if entrez_id not in gene_cache:
        #             gene = self.session.query(Gene).filter_by(entrez_id=str(entrez_id)).first()
        #             if gene:
        #                 gene_cache[entrez_id] = gene.id
        #             else:
        #                 continue

        #         gene_id = gene_cache[entrez_id]
        #         link = GeneVariantLink(gene_id=gene_id, variant_id=variant_id)
        #         self.session.merge(link)
        #         # key = (gene_id, variant_id)

        #         # if key not in gene_variant_seen:
        #         #     gene_variant_seen.add(key)
        #         #     links_to_insert.append(GeneVariantLink(
        #         #         gene_id=gene_id,
        #         #         variant_id=variant_id
        #         #     ))

        # # 🔄 Commit all batch inserts
        # # self.session.bulk_save_objects(locations_to_insert)
        # # self.session.bulk_save_objects(links_to_insert)
        self.session.commit()

        return len(variant_cache)