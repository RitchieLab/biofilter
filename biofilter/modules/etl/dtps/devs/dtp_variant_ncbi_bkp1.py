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
)
from biofilter.etl.dtps.worker_dbsnp import worker_dbsnp
from sqlalchemy import text
from sqlalchemy import any_, cast, Text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY

"""
This bkp was before change load module to dropp all previvous VariantLocus
"""


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

        def to_str_ids(xs):
            out = []
            for x in xs or []:
                if x is None:
                    continue
                out.append(str(x).strip())
            return out

        # ----= READ PROCESSED DATA =----
        # NOTE: # List all generated Parquet files.
        # Each file will be loaded individually in a loop.
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
        # relationship_type = (
        #     self.session.query(EntityRelationshipType)
        #     # .filter(EntityRelationshipType.code == "associated_with")
        #     .filter(code == "associated_with")
        #     # .one_or_none()
        # )
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
                    SELECT variant_id
                    FROM variant_masters
                    WHERE chromosome = :chrom
                """
                ),
                {"chrom": target_chrom},
            )
            self.existing_variant_ids = {
                str(row[0]) for row in result.fetchall()
            }  # noqa E501
            msg = f"🔍 {len(self.existing_variant_ids)} variants already in DB for chr {target_chrom}"  # noqa E501
            self.logger.log(msg, "INFO")

        except Exception as e:
            msg = f"❌ Failed to load existing variants from DB: {e}"
            self.logger.log(msg, "ERROR")
            self.existing_variant_ids = set()

        # ----= Set DB and drop indexes =----
        try:
            # self.db_write_mode()
            self.drop_indexes(self.get_variant_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # ===== PROCESS PER FILE =====
        # ============================
        for data_file in files_list:

            try:  # if error, go next file

                # -- Inside File Variables --
                gene_links_rows = []
                var_number = 0

                self.logger.log(f"📂 Processing {data_file}", "INFO")

                if self.debug_mode:
                    start_file = time.time()

                # -- Read Data --
                df = self._load_input_frame(data_file)
                if df.empty:
                    total_warnings += 1
                    msg = "File is empty."
                    self.logger.log(msg, "WARNING")
                    continue  # Go to next file

                # -- Drop variants without <<rs_id>>
                df = df[df["rs_id"].notna()].copy()

                # -- Check if Variant in DB --
                df["rs_id"] = df["rs_id"].astype(str)
                df = df[~df["rs_id"].isin(self.existing_variant_ids)]
                msg = f"➡️  Drop all Variants in DB, remaining: {len(df)}"
                self.logger.log(msg, "WARNING")

                # -- Add Accession and Assembly ID --
                df["assembly_id"] = df["seq_id"].map(acc2asm_id)
                df["chromosome"] = df["seq_id"].map(acc2chrom)
                # Drop variants without <<valid accession>>
                dropped_df = df[df["assembly_id"].isna()].copy()
                if not dropped_df.empty:
                    self.dropped_variants.append(dropped_df)
                df = df[df["assembly_id"].notna()].copy()
                msg = f"➡️  Drop all Variants withou valid Assembly, remaining {len(df)}"  # noqa E501
                self.logger.log(msg, "WARNING")

                # ------ PROCESS ROW / VARIANT ------
                # -----------------------------------
                for _, row in df.iterrows():

                    if self.debug_mode:
                        start_variant = time.time()

                    # --- Set Row Variables ---
                    vlocus_buffer = []
                    seen = set()

                    # --- Get Canonical Variant ID ---
                    variant_master = row.rs_id
                    chromossome = (
                        str(row.chromosome) if row.chromosome else None
                    )  # noqa E501

                    # ----== ENTITY DOMAIN ==----
                    # Add or Get Variant Master Entity
                    entity_id, is_new = self.get_or_create_entity(
                        force_create=True,
                        name=variant_master,
                        group_id=self.entity_group,
                        data_source_id=self.data_source.id,
                        package_id=self.package.id,
                        alias_type="rsID",  # TODO: CHECK W/TEAM
                        xref_source="dbSNP",  # TODO: CHECK W/TEAM
                        alias_norm=variant_master,
                        is_active=True,
                    )

                    # Always will be new!
                    if not is_new:
                        continue

                    # Add Merged Variants as Aliases to Virgent Variant
                    merged_list = row.merge_log or []
                    if not isinstance(merged_list, (list, tuple)):
                        merged_list = []
                    # normalize + dedup + remove rs equal the virgent
                    merged_list = {self._norm_rs(m) for m in merged_list}
                    merged_list.discard(None)
                    merged_list.discard(variant_master)
                    alias_dict = []
                    if merged_list:
                        for old_rs in merged_list:
                            alias_dict.append(
                                {
                                    "alias_value": old_rs,
                                    "alias_type": "merged",  # TODO: CHECK W/TEAM  # noqa E501
                                    "xref_source": "dbSNP",  # TODO: CHECK W/TEAM  # noqa E501
                                    "is_primary": False,
                                    "alias_norm": old_rs,
                                    "locale": "en",
                                }
                            )
                        # Add or Get EntityName

                        self.get_or_create_entity_name(
                            force_create=True,
                            group_id=self.entity_group,
                            entity_id=entity_id,
                            aliases=alias_dict,
                            is_active=False,
                            data_source_id=self.data_source.id,
                            package_id=self.package.id,
                        )

                    # ----== VARIANT DOMAIN ==----
                    # -->> CREATE VARIANT MASTER OBJECT
                    # - This is the Canonical Variant)
                    # - No check if exist before add
                    variant_master_obj = VariantMaster(
                        variant_id=variant_master,
                        variant_type=(
                            str(row["variant_type"]).upper()
                            if pd.notna(row["variant_type"])
                            else "SNP"
                        ),
                        omic_status_id="1",  # TODO It need to change?
                        chromosome=chromossome,  # noqa E501
                        quality=row["quality"],
                        entity_id=entity_id,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )

                    self.session.add(variant_master_obj)
                    self.session.flush()

                    # -->> CREATE VARIANT LOCUS TO ALL ASSEMBLIES
                    start = (
                        int(row["start_pos"]) if pd.notna(row["start_pos"]) else None
                    )
                    end = int(row["end_pos"]) if pd.notna(row["end_pos"]) else None
                    assembly_id = (
                        int(row["assembly_id"])
                        if pd.notna(row["assembly_id"])
                        else None
                    )

                    def normalize_allele_list(value):
                        # Caso esteja em string com falta de vírgula, corrige
                        if isinstance(value, str):
                            value = (
                                value.replace("'", "").replace("[", "").replace("]", "")
                            )  # noqa E501
                            return value.split()
                        elif isinstance(value, (list, np.ndarray)):
                            return list(map(str, value))
                        elif pd.isna(value) or value is None:
                            return []
                        return [str(value)]

                    alternate_allele = json.dumps(
                        normalize_allele_list(row["alt"])
                    )  # noqa E501
                    reference_allele = json.dumps(
                        normalize_allele_list(row["ref"])
                    )  # noqa E501

                    key = (
                        variant_master_obj.id,
                        assembly_id,
                        start,
                        end,
                    )  # noqa E501
                    if key not in seen:
                        seen.add(key)
                        vlocus_buffer.append(
                            VariantLocus(
                                variant_id=variant_master_obj.id,
                                assembly_id=assembly_id,
                                chromosome=chromossome,
                                start_pos=start,
                                end_pos=end,
                                reference_allele=reference_allele,
                                alternate_allele=alternate_allele,
                                data_source_id=self.data_source.id,
                                etl_package_id=self.package.id,
                            )
                        )

                    # Placements Locus
                    locus_map = {}
                    placements = row.get("placements") or []
                    for p in placements:
                        p_acc = p.get("seq_id")
                        if not p_acc:
                            continue

                        p_asm = assemblies_map.get(p_acc)
                        if not p_asm:
                            continue

                        p_start = p.get("start_pos")
                        p_end = p.get("end_pos")

                        if pd.isna(p_start) or pd.isna(p_end):
                            continue

                        p_start = int(p_start)
                        p_end = int(p_end)

                        key = (variant_master_obj.id, p_asm, p_start, p_end)
                        ref = p.get("ref")
                        alt = p.get("alt")

                        if not alt or alt == ref:
                            continue

                        if key not in locus_map:
                            locus_map[key] = {
                                "ref_set": set(),
                                "alt_set": set(),
                                "chromosome": acc2chrom.get(p_acc),
                            }

                        if ref:
                            locus_map[key]["ref_set"].add(str(ref))
                        if alt:
                            locus_map[key]["alt_set"].add(str(alt))

                    for (
                        variant_id,
                        asm_id,
                        start,
                        end,
                    ), vals in locus_map.items():  # noqa E501
                        ref_list = sorted(vals["ref_set"])
                        alt_list = sorted(vals["alt_set"])

                        vlocus_buffer.append(
                            VariantLocus(
                                variant_id=variant_id,
                                assembly_id=asm_id,
                                chromosome=vals["chromosome"],
                                start_pos=start,
                                end_pos=end,
                                reference_allele=json.dumps(ref_list),
                                alternate_allele=json.dumps(alt_list),
                                data_source_id=self.data_source.id,
                                etl_package_id=self.package.id,
                            )
                        )

                    # Add Locus Records
                    seen = set()
                    unique_vlocus = []
                    for v in vlocus_buffer:
                        key = (
                            v.variant_id,
                            v.assembly_id,
                            v.chromosome,
                            v.start_pos,
                            v.end_pos,
                            v.reference_allele,
                            v.alternate_allele,
                            v.data_source_id,
                            v.etl_package_id,
                        )
                        if key not in seen:
                            seen.add(key)
                            unique_vlocus.append(v)

                    vlocus_buffer = unique_vlocus
                    try:
                        if vlocus_buffer:
                            self.session.add_all(vlocus_buffer)
                            self.session.commit()
                            vlocus_buffer.clear()
                    except Exception as e:
                        msg = f"❌ Error to add data em Variants Locus: {str(e)}"  # noqa E501
                        self.logger.log(msg, "ERROR")
                        continue  # Go to the next Variant

                    """
                    Placement Field Templace
                        [{'alt': 'A', 'assembly': 'GRCh37.p13', 'end_pos': 19813529, 'ref': 'A', 'seq_id': 'NC_000008.10', 'start_pos': 19813529},  # noqa E501
                        {'alt': 'G', 'assembly': 'GRCh37.p13', 'end_pos': 19813529, 'ref': 'A', 'seq_id': 'NC_000008.10', 'start_pos': 19813529},  # noqa E501
                        {'alt': 'A', 'assembly': '', 'end_pos': 59302, 'ref': 'A', 'seq_id': 'NG_008855.2', 'start_pos': 59302},  # noqa E501
                        {'alt': 'G', 'assembly': '', 'end_pos': 59302, 'ref': 'A', 'seq_id': 'NG_008855.2', 'start_pos': 59302}  # noqa E501
                        ]
                    """

                    # -->> CREATE LIST TO ENTITIES LINKS TO GENES (rox)
                    # NOTE: Var x Gene will ingest in a single batch by file
                    genes_link = row.get("gene_links") or []
                    for g in genes_link:
                        gene_links_rows.append(
                            [variant_master, entity_id, g]
                        )  # rs, rs_entity_id, entrez_id

                    if self.debug_mode:
                        var_number += 1
                        end_variant = time.time() - start_variant
                        # Just print to save time
                        print(
                            f"Proccessed: {var_number} / To Process: {len(df) - var_number} in {end_variant:.5f}s: {variant_master}"
                        )  # noqa E501

            except Exception as e:
                msg = f"❌ Error to add data: {str(e)}"
                self.logger.log(msg, "ERROR")
                continue  # Go to the next File

            # ----== ENTITY DOMAIN: RELATIONSHIP==----
            try:
                # Reade all rows from a file
                if not gene_links_rows:
                    msg = f"🔗 No variant→gene links in {data_file}", "DEBUG"
                    self.logger.log(msg, "DEBUG")
                    continue  # Go to next File

                # DF (rs_id, rs_entity_id, entrez_id)
                df_links = pd.DataFrame(
                    gene_links_rows,
                    columns=["rs_id", "rs_entity_id", "entrez_id"],
                ).drop_duplicates()

                # Search GeneMaster from EntrezID
                entrez_list = (
                    df_links["entrez_id"]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()  # noqa E501
                )

                # TODO: Talvez buscar no inicio todos os Genes do Chromossome!?
                # alias_rows = (
                #     self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)  # noqa E501
                #     .filter(EntityAlias.alias_type == "code")
                #     .filter(EntityAlias.xref_source == "ENTREZ")
                #     .filter(EntityAlias.alias_value.in_(entrez_list))
                #     .all()
                # )

                entrez_list_str = to_str_ids(entrez_list)

                if not entrez_list_str:
                    alias_rows = []
                else:
                    alias_rows = (
                        self.session.query(
                            EntityAlias.alias_value, EntityAlias.entity_id
                        )
                        .filter(
                            EntityAlias.alias_type == "code",
                            EntityAlias.xref_source == "ENTREZ",
                        )
                        .filter(
                            EntityAlias.alias_value
                            == any_(
                                cast(
                                    bindparam("vals", value=entrez_list_str),
                                    ARRAY(Text),
                                )
                            )
                        )
                        .all()
                    )

                entrez2eid = {
                    row.alias_value: row.entity_id for row in alias_rows
                }  # noqa E501
                df_genes = pd.DataFrame(
                    list(entrez2eid.items()),
                    columns=["entrez_id_str", "gene_entity_id"],
                )
                df_genes["entrez_id"] = (
                    df_genes["entrez_id_str"].astype(str).astype(int)
                )
                df_genes = df_genes.drop(columns=["entrez_id_str"])

                # Merge Variants x Genes Informations
                df_merge = df_links.merge(
                    df_genes, on="entrez_id", how="left"
                )  # noqa E501

                found = df_merge[df_merge["gene_entity_id"].notna()].copy()
                missing = df_merge[
                    df_merge["gene_entity_id"].isna()
                ].copy()  # noqa E501

                if not found.empty:
                    found["rs_entity_id"] = found["rs_entity_id"].astype(  # noqa E501
                        int
                    )
                    found["gene_entity_id"] = found[
                        "gene_entity_id"
                    ].astype(  # noqa E501
                        int
                    )  # noqa E501

                    found = found.drop_duplicates(
                        subset=["rs_entity_id", "gene_entity_id"]
                    )

                    rel_buffer = []
                    for r in found.itertuples(index=False):
                        rel_buffer.append(
                            EntityRelationship(
                                entity_1_id=int(r.rs_entity_id),
                                entity_1_group_id=self.entity_group,
                                entity_2_id=int(r.gene_entity_id),
                                entity_2_group_id=gene_group.id,
                                relationship_type_id=relationship_type.id,
                                data_source_id=self.data_source.id,
                                etl_package_id=self.package.id,
                            )
                        )

                    # Insert in Bucker
                    BATCH = 50_000
                    total = 0
                    for i in range(0, len(rel_buffer), BATCH):
                        chunk = rel_buffer[i : i + BATCH]  # noqa E203
                        self.session.bulk_save_objects(chunk)
                        try:
                            self.session.commit()
                        except Exception as e:
                            self.session.rollback()
                            self.logger.log(
                                f"⚠️ commit failed inserting relationships ({os.path.basename(data_file)}): {e}",  # noqa E501
                                "WARNING",
                            )
                        total += len(chunk)

                    self.logger.log(
                        f"🔗 Inserted {total} EntityRelationship(s) from {os.path.basename(data_file)}",  # noqa E501
                        "INFO",
                    )
                else:
                    self.logger.log(
                        f"🔗 No resolvable gene entities in {os.path.basename(data_file)}",  # noqa E501
                        "INFO",
                    )

                # Save all genes not found
                if not missing.empty:
                    missing_file = str(
                        "missing_variant_gene_entities_"
                        + os.path.basename(data_file).replace(
                            ".parquet", ""
                        )  # noqa E501
                        + ".csv"
                    )
                    missing_file = str(processed_path / missing_file)
                    missing[
                        ["rs_id", "entrez_id"]
                    ].drop_duplicates().to_csv(  # noqa E501
                        missing_file, index=False
                    )
                    self.logger.log(
                        f"⚠️ Missing gene entities saved: {missing_file}",
                        "WARNING",  # noqa E501
                    )

                if self.debug_mode:
                    msg = f"File {data_file} ingested in {time.time() - start_file}"  # noqa E501
                    self.logger.log(msg, "DEBUG")

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
        try:
            self.create_indexes(self.get_variant_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

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
