# dtp_variant_ncbi_v2.py
import os
import re
import bz2
import glob
import sys
import __main__
import time
import pandas as pd
import numpy as np
import json
from typing import Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy.exc import IntegrityError
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    GenomeAssembly,
    VariantMaster,
    VariantLocus,
    EntityGroup,
    EntityAlias,
    EntityRelationship,
    EntityRelationshipType,
    Entity,
)
from biofilter.etl.dtps.worker_dbsnp import worker_dbsnp
from sqlalchemy import text
from sqlalchemy import any_, cast, Text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_variant_ncbi"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """
        msg = f"Starting extraction of {self.data_source.name} data..."

        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        # if force_steps:
        #     last_hash = ""
        #     msg = "Ignoring hash check."
        #     self.logger.log(msg, "WARNING")
        # else:
        #     last_hash = self.etl_process.raw_data_hash

        try:
            # Landing path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )

            # Get hash from current md5 file
            url_md5 = f"{source_url}.md5"
            current_hash = self.get_md5_from_url_file(url_md5)

            # if not current_hash:
            #     msg = f"Failed to retrieve MD5 from {url_md5}"
            #     self.logger.log(msg, "WARNING")
            #     return False, msg, None

            # # Compare current hash and last processed hash
            # if current_hash == last_hash:
            #     msg = f"No change detected in {source_url}"
            #     self.logger.log(msg, "INFO")
            #     return False, msg, current_hash

            # Download the file
            status, msg = self.http_download(source_url, landing_path)

            if not status:
                self.logger.log(msg, "ERROR")
                return False, msg, current_hash

            # Finish block
            msg = f"✅ {self.data_source.name} file downloaded to {landing_path}"  # noqa: E501
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):

        msg = f"🔧 Transforming the {self.data_source.name} data ..."

        self.logger.log(msg, "INFO")  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        try:
            input_file = self.get_raw_file(raw_dir)
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            output_dir = self.get_path(processed_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            for f in output_dir.iterdir():
                if f.name.endswith(".parquet"):
                    f.unlink()

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # parameters
        batch_size = 200_000
        max_workers = 1  # TODO: Implementar metodos melhores

        try:
            # NOTE: I have problem with MP on VPS. Need review

            # futures, batch, batch_id = [], [], 0
            # with bz2.open(
            #     input_file, "rt", encoding="utf-8"
            # ) as f, ProcessPoolExecutor(  # noqa E501
            #     max_workers=max_workers
            # ) as ex:
            #     if __name__ == "__main__" or (
            #         hasattr(__main__, "__file__") and not hasattr(sys, "ps1")
            #     ):
            #         for line in f:
            #             batch.append(line)
            #             if len(batch) >= batch_size:
            #                 futures.append(
            #                     ex.submit(
            #                         worker_dbsnp,
            #                         batch.copy(),
            #                         batch_id,
            #                         output_dir,
            #                     )
            #                 )
            #                 batch.clear()
            #                 batch_id += 1
            #         if batch:
            #             futures.append(
            #                 ex.submit(
            #                     worker_dbsnp,
            #                     batch.copy(),
            #                     batch_id,
            #                     output_dir,
            #                 )
            #             )

            #         for fut in as_completed(futures):
            #             fut.result()
            #     else:
            #         self.logger.log(
            #             "⚠️ Skipping multiprocessing: not in __main__ context.",  # noqa E501
            #             "WARNING",
            #         )

            batch, batch_id = [], 0

            # (opcional) pular partes já prontas p/ retomar
            def already_done(pid: int) -> bool:
                # ajuste se seu worker usa outro padrão de nome
                return os.path.exists(
                    os.path.join(output_dir, f"processed_part_{pid}.parquet")
                )

            with bz2.open(input_file, "rt", encoding="utf-8") as f:
                self.logger.log(
                    "🧵 Running in SERIAL mode (no multiprocessing).", "INFO"
                )

                for line in f:
                    batch.append(line)
                    if len(batch) >= batch_size:
                        if not already_done(batch_id):
                            worker_dbsnp(batch, batch_id, output_dir)  # chamada direta
                        else:
                            self.logger.log(
                                f"⏭️  Skipping existing part {batch_id}", "DEBUG"
                            )
                        batch_id += 1
                        batch = []  # libera memória

                # resto final
                if batch:
                    if not already_done(batch_id):
                        worker_dbsnp(batch, batch_id, output_dir)
                    else:
                        self.logger.log(
                            f"⏭️  Skipping existing part {batch_id}", "DEBUG"
                        )
                    batch_id += 1
                    batch = []

            # msg = f"✅ Processing completed with {len(futures)} batches."
            msg = f"✅ Processing completed with {batch_id} batches (serial)."
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # --- Support methods ---

    def _to_py(self, x):
        """Converte strings que representam listas/dicts para objeto Python."""
        if isinstance(x, np.ndarray):
            x = x.tolist()
        if isinstance(x, (list, dict)) or x is None:
            return x

    def _load_input_frame(self, path: str) -> pd.DataFrame:
        if path.endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path, sep=",")
        expected = [
            "rs_id",
            "variant_type",
            "build_id",
            "seq_id",
            "assembly",
            "start_pos",
            "end_pos",
            "ref",
            "alt",
            "placements",
            "merge_log",
            "gene_links",
            "quality",
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            raise ValueError(f"Input file {path} missing columns: {missing}")

        for c in ["start_pos", "end_pos", "build_id"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        df["placements"] = df["placements"].apply(self._to_py)
        df["merge_log"] = df["merge_log"].apply(self._to_py)
        df["gene_links"] = df["gene_links"].apply(self._to_py)

        # alt can come as a list of strings; normalize to string "A/T"
        def _alt_str(x):
            if isinstance(x, list):
                return "/".join(
                    sorted(
                        {str(a) for a in x if a is not None and str(a) != ""}
                    )  # noqa E501
                )
            if x is None:
                return ""
            return str(x)

        df["alt"] = df["alt"].apply(_alt_str)
        df["ref"] = df["ref"].fillna("").astype(str)

        # In the absence of placement or empty lists, use []
        for c in ["placements", "merge_log", "gene_links"]:
            df[c] = df[c].apply(lambda v: v if isinstance(v, list) else [])

        return df

    def _norm_rs(self, x: str) -> str | None:
        if not x:
            return None
        s = str(x).strip()
        # Accept "RS123", "rs123", "  rs123  "
        if s.lower().startswith("rs") and s[2:].isdigit():
            return f"rs{int(s[2:])}"
        # Some dumps come with just the number
        if s.isdigit():
            return f"rs{int(s)}"
        return None

    def _norm_chr(s: str | None) -> str | None:
        if not s:
            return None
        x = str(s).strip().upper()
        if x.startswith("CHR"):
            x = x[3:]
        if x in {"23", "X"}:
            return "X"
        if x in {"24", "Y"}:
            return "Y"
        if x in {"M", "MT", "MITO", "MITOCHONDRIAL"}:
            return "MT"
        return x  # "1".."22", "X","Y","MT"

    def _ensure_list(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []
        if isinstance(x, (list, tuple, set)):
            return list(x)
        return [x]

    def _extract_chrom_from_ds(self, ds_name: str) -> str | None:
        # Extrai o final após o "_"
        raw = ds_name.lower().split("_")[-1]

        # Mapeamento opcional para nomes não padronizados
        mapping = {
            "chrx": "X",
            "chry": "Y",
            "chrmt": "MT",
            "chrm": "MT",
        }

        # Se estiver no dicionário, retorna direto
        if raw in mapping:
            return mapping[raw]

        # Se for "chr" seguido de número, extrai
        match = re.match(r"chr?(\d+)$", raw)
        if match:
            return match.group(1)

        # Caso contrário, tenta retornar como está (se for válido)
        if raw in [str(i) for i in range(1, 23)] + ["x", "y", "mt"]:
            return raw

        return None  # Não identificado

    # Keep Alleles as List of String in DB.
    def _normalize_allele(self, val):
        if isinstance(val, str):
            val = val.replace("'", "").replace("[", "").replace("]", "")
            return json.dumps(val.split())
        elif isinstance(val, (list, np.ndarray)):
            return json.dumps(list(map(str, val)))
        elif pd.isna(val) or val is None:
            return json.dumps([])
        return json.dumps([str(val)])

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        # Setting variables to loader
        total_variants = 0
        total_warnings = 0
        self.dropped_variants = []

        if self.debug_mode:
            start_total = time.time()

        # ----= READ PROCESSED DATA =----
        # NOTE: # List all generated Parquet files.
        try:
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR
            processed_path = self.get_path(processed_dir)
            files_list = sorted(
                glob.glob(str(processed_path / "processed_part_*.parquet"))
            )
            if not files_list:
                msg = f"No part files found in {processed_path}"
                self.logger.log(msg, "ERROR")
                return False, msg
            msg = f"📄 Found {len(files_list)} part files to load"
            self.logger.log(msg, "INFO")
        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # ----= GET ENTITY GROUP (Variant and Gene) =----
        try:
            # Variant group (sets self.entity_group_id)
            self.get_entity_group("Variants")
            # Gene group (used for alias resolution)
            gene_group = (
                self.session.query(EntityGroup).filter_by(name="Genes").first()
            )  # noqa E501
            if not gene_group:
                raise ValueError("EntityGroup 'Genes' not found in database.")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        # ----= GET RELATIONSHIP TYPE =----
        relationship_type = (
            self.session.query(EntityRelationshipType)
            .filter_by(code="associated_with")
            .first()
        )
        if not relationship_type:
            relationship_type = EntityRelationshipType(
                code="associated_with",
                description="Auto-created by variant DTP",  # noqa E501
            )
            self.session.add(relationship_type)
            self.session.commit()

        # ---= GET ASSEMBLIES =----
        assemblies = self.session.query(GenomeAssembly).all()
        assemblies_map = {asm.accession: asm.id for asm in assemblies}
        acc2asm_id: Dict[str, int] = {a.accession: a.id for a in assemblies}
        acc2chrom: Dict[str, str] = {
            a.accession: (a.chromosome or "") for a in assemblies
        }

        # ---= Get exist Vars in DB by Chrom =---
        try:
            target_chrom = self._extract_chrom_from_ds(
                self.data_source.name
            )  # noqa E501
            result = self.session.execute(
                text(
                    """
                    SELECT rs_id, entity_id
                    FROM variant_masters
                    WHERE chromosome = :chrom
                """
                ),
                {"chrom": target_chrom},
            )
            if result:
                self.existing_variants_dict = dict(result.fetchall())
                msg = f"🔍 {len(self.existing_variants_dict)} variants already in DB for chr {target_chrom}"  # noqa E501
                self.logger.log(msg, "INFO")

        except Exception as e:
            msg = f"❌ Failed to load existing variants from DB: {e}"
            self.logger.log(msg, "WARNING")
            self.existing_variant_ids = set()

        # ---= Get Dict of Genes by Entrez ID and Entity ID =---
        try:
            result_gene = self.session.execute(
                text(
                    """
                    SELECT alias_value, entity_id
                    FROM entity_aliases
                    WHERE group_id = :group
                    AND alias_type = 'code'
                    AND xref_source = 'ENTREZ'
                """
                ),
                {"group": gene_group.id},
            )
            if result_gene:
                self.gene_lookup_dict = dict(result_gene.fetchall())

        except Exception as e:
            # NOTE: Future we can run without genes
            msg = f"❌ Failed to create a list of Genes: {e}"
            return False, msg

        # ----= Set DB and drop indexes =----
        try:
            # self.db_write_mode()
            self.drop_indexes(self.get_variant_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg

        # ===== PROCESS PER FILE =====
        # ============================
        for data_file in files_list:

            try:  # if error, go next file
                if self.debug_mode:
                    start_file = time.time()

                # ----= Setting File Variables =----
                gene_links_rows = []
                var_number = 0
                var_drop_number = 0
                self.logger.log(f"📂 Processing {data_file}", "INFO")

                # ----= Read Data =----
                df = self._load_input_frame(data_file)
                if df.empty:
                    total_warnings += 1
                    msg = "File is empty."
                    self.logger.log(msg, "WARNING")
                    continue  # Go to next file

                # -- Drop variants without <<rs_id>>
                nb = len(df)
                df = df[df["rs_id"].notna()].copy()
                df["rs_id"] = df["rs_id"].astype(str)
                nf = nb - len(df)
                if nf > 0:
                    var_drop_number += nf
                    msg = f"{nf} variants dropped because they are missing an rs_id"
                    self.logger.log(msg, "WARNING")

                # -- Check if Variant in DB --
                # Add Varaint Entity ID column with previous DB records
                df["entity_id"] = (
                    df["rs_id"].map(self.existing_variants_dict).astype("Int64")
                )
                n_existing = df["entity_id"].notna().sum()
                n_new = df["entity_id"].isna().sum()
                df = df[df["entity_id"].isna()].copy()

                if n_existing > 0:
                    self.logger.log(
                        f"Found {n_existing} already in DB, {n_new} new variants to process",
                        "WARNING",
                    )
                    var_drop_number += n_existing
                if df.empty:
                    self.logger.log(
                        "No more variants to ingest — skipping to next file", "WARNING"
                    )
                    continue

                # -- Add Accession and Assembly ID --
                df["assembly_id"] = df["seq_id"].map(acc2asm_id)
                df["chromosome"] = df["seq_id"].map(acc2chrom)

                # -- Drop variants without <<valid accession>> --
                # NOTE: Future create a list of drop variant with reason
                # dropped_df = df[df["assembly_id"].isna()].copy()
                # if not dropped_df.empty:
                #     self.dropped_variants.append(dropped_df)
                nb = len(df)
                df = df[df["assembly_id"].notna()].copy()
                nf = nb - len(df)
                if nf > 0:
                    var_drop_number += nf
                    msg = f"{nf} variants dropped without valid Assembly, remaining {len(df)}"  # noqa E501
                    self.logger.log(msg, "WARNING")
                if df.empty:
                    self.logger.log(
                        "No more variants to ingest — skipping to next file", "WARNING"
                    )
                    continue

                # ----= Map Gene Entrez ID --> Gene Entity ID =----
                def map_entrez_to_entity(entrez_list, gene_dict):
                    return [
                        gene_dict.get(str(e))
                        for e in entrez_list
                        if str(e) in gene_dict
                    ]

                df["gene_entity_ids"] = df["gene_links"].apply(
                    lambda lst: map_entrez_to_entity(lst, self.gene_lookup_dict)
                )

                # ----= Normalizing columns before ingestion =----
                df["ref_json"] = df["ref"].apply(self._normalize_allele)
                df["alt_json"] = df["alt"].apply(self._normalize_allele)

                df["start_pos"] = df["start_pos"].apply(
                    lambda x: int(x) if pd.notna(x) else None
                )
                df["end_pos"] = df["end_pos"].apply(
                    lambda x: int(x) if pd.notna(x) else None
                )
                df["assembly_id"] = df["assembly_id"].apply(
                    lambda x: int(x) if pd.notna(x) else None
                )

                # ===== START DATABASE INSERTION =====
                # ====================================

                # ----= Insert Variants as Entities in bulk (use add_all to get id) =----
                entities_to_insert = [
                    Entity(
                        group_id=self.entity_group,
                        has_conflict=False,
                        is_active=True,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )
                    for _ in df.itertuples()
                ]
                # --- Check if all was inserted ---
                assert len(df) == len(
                    entities_to_insert
                ), "Mismatch between DataFrame and inserted Entities"
                self.session.add_all(entities_to_insert)
                self.session.flush()

                # --- Create Variant Entity ID Column ---
                df["entity_id"] = [ent.id for ent in entities_to_insert]

                # ---= Inserir Variants as Master in bulk =----
                variant_objects = [
                    VariantMaster(
                        rs_id=row.rs_id,
                        variant_type=row.variant_type,
                        omic_status_id=1,
                        chromosome=row.chromosome,
                        quality=row.quality,
                        entity_id=row.entity_id,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )
                    for row in df.itertuples()
                ]
                # --- Check if all was inserted ---
                assert len(df) == len(
                    variant_objects
                ), "Mismatch between DataFrame and inserted Variant Master"
                self.session.add_all(variant_objects)
                self.session.flush()

                # --- Create Variant Master ID Column ---
                rsid_to_variant_master_id = {
                    variant.rs_id: variant.id for variant in variant_objects
                }
                df["variant_master_id"] = df["rs_id"].map(rsid_to_variant_master_id)

                # BUG: Parei aqui!

                # INSERIR ALIAS
                merged_aliases_to_insert = []
                aliases_to_insert = []
                locus_records = []
                placement_locus = []
                relationships_to_insert = []

                # UNIQUE INTERACTION TO ALL MODELS
                for row in df.itertuples():

                    # 1. ENTITY ALIASES
                    # Alias Variant Primary
                    aliases_to_insert.append(
                        EntityAlias(
                            entity_id=row.entity_id,
                            group_id=self.entity_group,
                            alias_value=row.rs_id,
                            alias_type="rsID",
                            xref_source="dbSNP",
                            is_primary=True,
                            is_active=True,
                            alias_norm=row.rs_id.lower(),
                            data_source_id=self.data_source.id,
                            etl_package_id=self.package.id,
                        )
                    )

                    # Alias Variants Merged
                    merged = getattr(row, "merge_log", []) or []
                    if isinstance(merged, str):
                        merged = json.loads(merged)
                    if merged:
                        for merged_rsid in merged:
                            # if merged_rsid != row.rs_id:  # just case
                            merged_aliases_to_insert.append(
                                EntityAlias(
                                    entity_id=row.entity_id,
                                    group_id=self.entity_group,
                                    alias_value=merged_rsid,
                                    alias_type="merged",
                                    xref_source="dbSNP",
                                    is_primary=False,
                                    is_active=False,
                                    alias_norm=merged_rsid.lower(),
                                    data_source_id=self.data_source.id,
                                    etl_package_id=self.package.id,
                                )
                            )

                    # --Inserir VariantLocus
                    build = row.assembly
                    build = build.replace("GRCh", "").split(".")[0]  # '38'

                    locus_records.append(
                        VariantLocus(
                            variant_id=row.variant_master_id,
                            rs_id=row.rs_id,
                            entity_id=row.entity_id,
                            build=build,
                            assembly_id=row.assembly_id,
                            chromosome=row.chromosome,
                            start_pos=row.start_pos,
                            end_pos=row.end_pos,
                            reference_allele=row.ref_json,
                            alternate_allele=row.alt_json,
                            data_source_id=self.data_source.id,
                            etl_package_id=self.package.id,
                        )
                    )

                    # - Processar placements (se existirem)
                    # placements = getattr(row, "placements", []) or []
                    # for p in placements:
                    #     p_acc = p.get("seq_id")
                    #     asm_id = assemblies_map.get(p_acc)
                    #     if not asm_id:
                    #         continue
                    #     p_start = p.get("start_pos")
                    #     p_end = p.get("end_pos")
                    #     if not p_start or not p_end:
                    #         continue

                    #     ref = p.get("ref")
                    #     alt = p.get("alt")
                    #     if not alt or alt == ref:
                    #         continue

                    #     chrom = acc2chrom.get(p_acc)
                    #     ref_json = json.dumps([str(ref)]) if ref else json.dumps([])
                    #     alt_json = json.dumps([str(alt)]) if alt else json.dumps([])

                    #     build = p.get("assembly")
                    #     build = build.replace("GRCh", "").split(".")[0]

                    #     placement_locus.append(
                    #         VariantLocus(
                    #             variant_id=row.variant_master_id,
                    #             rs_id=row.rs_id,
                    #             entity_id=row.entity_id,
                    #             build=build,
                    #             assembly_id=asm_id,
                    #             chromosome=chrom,
                    #             start_pos=int(p_start),
                    #             end_pos=int(p_end),
                    #             reference_allele=ref_json,
                    #             alternate_allele=alt_json,
                    #             data_source_id=self.data_source.id,
                    #             etl_package_id=self.package.id,
                    #         )
                    #     )
                    placements = getattr(row, "placements", []) or []

                    # Acumulador por locus: (assembly_id, chrom, start, end, ref) -> {build, alts:set}
                    agg = {}

                    for p in placements:
                        p_acc = p.get("seq_id")
                        asm_id = assemblies_map.get(p_acc)
                        if not asm_id:
                            continue

                        p_start = p.get("start_pos")
                        p_end = p.get("end_pos")
                        if not p_start or not p_end:
                            continue

                        ref = p.get("ref")
                        alt = p.get("alt")
                        # ignorar alt vazio ou igual ao ref (sem variação)
                        if not alt or alt == ref:
                            continue

                        chrom = acc2chrom.get(p_acc)
                        if not chrom:
                            continue

                        # build: "GRCh38.p14" -> "38"
                        build = p.get("assembly") or ""
                        build = build.replace("GRCh", "").split(".")[0]

                        key = (asm_id, chrom, int(p_start), int(p_end), str(ref or ""))

                        bucket = agg.get(key)
                        if bucket is None:
                            bucket = {"build": build, "alts": set()}
                            agg[key] = bucket

                        bucket["alts"].add(str(alt))

                    # Agora, gerar UMA linha por locus com alts agregados
                    for (asm_id, chrom, start, end, ref), bucket in agg.items():
                        ref_json = json.dumps([str(ref)]) if ref else json.dumps([])
                        # ordena para estabilidade determinística
                        alt_json = json.dumps(sorted(bucket["alts"]))

                        placement_locus.append(
                            VariantLocus(
                                variant_id=row.variant_master_id,
                                rs_id=row.rs_id,
                                entity_id=row.entity_id,
                                build=bucket["build"],
                                assembly_id=asm_id,
                                chromosome=chrom,
                                start_pos=start,
                                end_pos=end,
                                reference_allele=ref_json,
                                alternate_allele=alt_json,
                                data_source_id=self.data_source.id,
                                etl_package_id=self.package.id,
                            )
                        )

                    # Gene - Varaiants Links

                    # Pode ser string separada por vírgula, lista de ints ou até NaN
                    gene_ids = getattr(row, "gene_entity_ids", []) or []
                    # # Converte se for string (ex: "123,456")
                    # if isinstance(gene_ids, str):
                    #     gene_ids = [int(g.strip()) for g in gene_ids.split(",") if g.strip().isdigit()]
                    # Garante lista
                    # if not isinstance(gene_ids, list):
                    #     continue

                    for gene_entity_id in gene_ids:
                        relationships_to_insert.append(
                            EntityRelationship(
                                entity_1_id=row.entity_id,  # Variant Entity ID
                                entity_1_group_id=self.entity_group,
                                entity_2_id=gene_entity_id,
                                entity_2_group_id=gene_group.id,
                                relationship_type_id=relationship_type.id,
                                data_source_id=self.data_source.id,
                                etl_package_id=self.package.id,
                            )
                        )

                # Insert all List to DB
                self.session.bulk_save_objects(aliases_to_insert)
                self.session.bulk_save_objects(merged_aliases_to_insert)
                if relationships_to_insert:
                    self.session.bulk_save_objects(relationships_to_insert)

                all_loci = locus_records + placement_locus
                # Eliminar duplicados (pela chave natural)
                seen = set()
                unique_loci = []
                for loc in all_loci:
                    key = (
                        loc.variant_id,
                        loc.rs_id,
                        loc.entity_id,
                        loc.build,
                        loc.assembly_id,
                        loc.chromosome,
                        loc.start_pos,
                        loc.end_pos,
                        loc.reference_allele,
                        loc.alternate_allele,
                        loc.data_source_id,
                        loc.etl_package_id,
                    )
                    if key not in seen:
                        seen.add(key)
                        unique_loci.append(loc)
                self.session.bulk_save_objects(unique_loci)

                # TODO: no chromossomo Y temos dados do X

                # manda para a DB
                self.session.commit()

            except IntegrityError as e:
                self.session.rollback()
                total_warnings += 1
                self.logger.log(
                    f"⚠️ Integrity error while loading {os.path.basename(data_file)}: {e}",  # noqa E501
                    "WARNING",
                )
            except Exception as e:
                self.session.rollback()
                total_warnings += 1
                self.logger.log(
                    f"⚠️ Unexpected error while loading {os.path.basename(data_file)}: {e}",  # noqa E501
                    "WARNING",
                )

        # Set DB to Read Mode and Create Index
        # try:
        #     self.create_indexes(self.get_variant_index_specs)
        #     self.create_indexes(self.get_entity_index_specs)
        #     self.db_read_mode()
        # except Exception as e:
        #     total_warnings += 1
        #     msg = f"Failed to switch DB to write mode or drop indexes: {e}"
        #     self.logger.log(msg, "WARNING")

        if self.debug_mode:
            msg = f"Load process ran in {time.time() - start_total}"
            self.logger.log(msg, "DEBUG")

        # - Salve all Variants Dropped in que QA Process
        if self.dropped_variants:
            all_dropped = pd.concat(self.dropped_variants, ignore_index=True)
            dropped_vars_file = (
                f"dropped_variants__package_{self.package.id}.csv"  # noqa E501
            )
            output_path = str(processed_path / dropped_vars_file)
            all_dropped.to_csv(output_path, index=False)
            self.logger.log(
                f"📤 Saved dropped variants to: {output_path}", "INFO"
            )  # noqa E501

        if total_warnings == 0:
            msg = f"✅ Loaded {total_variants} variants from {len(files_list)} file(s)."  # noqa E501
            self.logger.log(msg, "SUCCESS")
            return True, msg
        else:
            msg = f"Loaded {total_variants} variants with {total_warnings} warning(s). Check logs."  # noqa E501
            self.logger.log(msg, "WARNING")
            return True, msg
