# dtp_variant_ncbi_v2.py
import os
import bz2
import glob
import sys
import __main__

# import time  # DEBUG
import pandas as pd
import numpy as np
from typing import Dict
from concurrent.futures import ProcessPoolExecutor, as_completed

# from pathlib import Path
from sqlalchemy.exc import IntegrityError

# from collections import defaultdict
# from sqlalchemy import and_

from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    GenomeAssembly,
    GeneMaster,
    VariantMaster,
    VariantLocus,
    EntityGroup,
    EntityRelationship,
    EntityRelationshipType,
)
from biofilter.etl.dtps.worker_dbsnp import worker_dbsnp


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.data_source = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_variant_ncbi"
        self.dtp_version = "1.0.1"
        self.compatible_schema_min = "3.0.1"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """
        msg = f"Starting extraction of {self.data_source.name} data..."

        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check."
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

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

            if not current_hash:
                msg = f"Failed to retrieve MD5 from {url_md5}"
                self.logger.log(msg, "WARNING")
                return False, msg, None

            # Compare current hash and last processed hash
            if current_hash == last_hash:
                msg = f"No change detected in {source_url}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

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
    def transform(self, raw_path, processed_path):
        self.logger.log(
            f"🔧 Transforming the {self.data_source.name} data ...", "INFO"
        )  # noqa E501
        self.check_compatibility()

        input_file = self.get_raw_file(raw_path)
        if not input_file.exists():
            msg = f"❌ Input file not found: {input_file}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

        output_dir = self.get_path(processed_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        for f in output_dir.iterdir():
            if f.name.endswith(".parquet"):
                f.unlink()

        # parameters
        batch_size = 200_000
        max_workers = 6

        try:
            futures, batch, batch_id = [], [], 0
            with bz2.open(
                input_file, "rt", encoding="utf-8"
            ) as f, ProcessPoolExecutor(  # noqa E501
                max_workers=max_workers
            ) as ex:
                if __name__ == "__main__" or (
                    hasattr(__main__, "__file__") and not hasattr(sys, "ps1")
                ):
                    for line in f:
                        batch.append(line)
                        if len(batch) >= batch_size:
                            futures.append(
                                ex.submit(
                                    worker_dbsnp,
                                    batch.copy(),
                                    batch_id,
                                    output_dir,
                                )
                            )
                            batch.clear()
                            batch_id += 1
                    if batch:
                        futures.append(
                            ex.submit(
                                worker_dbsnp,
                                batch.copy(),
                                batch_id,
                                output_dir,
                            )
                        )

                    for fut in as_completed(futures):
                        fut.result()
                else:
                    self.logger.log(
                        "⚠️ Skipping multiprocessing: not in __main__ context.",  # noqa E501
                        "WARNING",
                    )

            msg = f"✅ Processing completed with {len(futures)} batches."
            self.logger.log(msg, "INFO")
            return None, True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥

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

    # ---- LOADER METHOD ----
    def load(
        self,
        df=None,
        processed_path=None,
    ):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        # Setting variables to loader
        # data_source_id = self.data_source.id
        total_variants = 0
        total_warnings = 0
        load_status = False
        # missing_rows = []

        # DEBUG
        # start_total = time.time()

        index_specs = [
            ("variant_masters", ["variant_id"]),
            ("variant_masters", ["data_source_id"]),
            ("variant_loci", ["variant_id", "assembly_id"]),
        ]
        try:
            self.db_write_mode()
            self.drop_indexes(index_specs)
        except Exception as e:
            msg = f"⚠️ Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        # Search Processed Files
        if df is None:
            if not processed_path:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            processed_path = self.get_path(processed_path)
            csv_files = sorted(
                glob.glob(str(processed_path / "processed_part_*.parquet"))
            )

            if not csv_files:
                msg = f"No part files found in {processed_path}"
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            self.logger.log(
                f"📄 Found {len(csv_files)} part files to load", "INFO"
            )  # noqa: E501

        # Get Entity Group ID
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = (
                self.session.query(EntityGroup)
                .filter_by(name="Variants")
                .first()  # noqa: E501
            )  # noqa: E501
            if not group:
                msg = "EntityGroup 'Proteomics' not found in the database."
                self.logger.log(msg, "ERROR")
                return total_variants, False
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Variants' is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

        # Get Entity RelationShip TYPE ID
        REL_TYPE_NAME = "associated_with"

        # prepare relationship_type_id once (can be outside of file loop)
        rel_type = (
            self.session.query(EntityRelationshipType)
            .filter(EntityRelationshipType.code == REL_TYPE_NAME)
            .one_or_none()
        )
        if not rel_type:
            rel_type = EntityRelationshipType(
                code=REL_TYPE_NAME, description="Auto-created by variant DTP"
            )
            self.session.add(rel_type)
            self.session.commit()
        rel_type_id = rel_type.id

        # Maps
        assemblies = self.session.query(GenomeAssembly).all()
        assemblies_map = {asm.accession: asm.id for asm in assemblies}
        acc2asm_id: Dict[str, int] = {a.accession: a.id for a in assemblies}
        acc2chrom: Dict[str, str] = {
            a.accession: (a.chromosome or "") for a in assemblies
        }

        # Process ingestion by file
        for csv_file in csv_files:

            try:

                gene_links_rows = []

                self.logger.log(f"📂 Processing {csv_file}", "INFO")

                # Read data from PARQUET files
                df = self._load_input_frame(csv_file)

                # Check if DataFrame is empty
                if df.empty:
                    msg = "DataFrame is empty."
                    self.logger.log(msg, "ERROR")
                    return total_variants, False, msg

                # Interaction to each Variant entry
                for _, row in df.iterrows():

                    # DEBUG: Add time here

                    # Get canonical assembly ID
                    acc = str(row["seq_id"])
                    asm_id = acc2asm_id.get(acc)
                    if not asm_id:
                        total_warnings += 1
                        self.logger.log(
                            f"⚠️ Unknown accession (GenomeAssembly): {acc} (rs={row['rs_id']}) — skipping variant",  # noqa E501
                            "WARNING",
                        )
                        continue

                    # 1) VIRGENT AND ALIASES ENTITY VARIANT
                    # Add or Get Entity for Canonical Variant
                    variant_master = self._norm_rs(row.rs_id)

                    # Skip if no Variant master ID
                    if not variant_master:
                        msg = f"Variant Master not found in row: {row}"
                        self.logger.log(msg, "WARNING")
                        continue
                    # Add or Get Variant Master Entity
                    entity_id, is_new = self.get_or_create_entity(
                        name=variant_master,
                        group_id=self.entity_group,
                        data_source_id=self.data_source.id,
                    )

                    # TODO: 3.0.1 Only add as incremental
                    #       Planning to updates method in future version
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

                    for old_rs in merged_list:
                        self.get_or_create_entity_name(
                            entity_id,
                            old_rs,
                            data_source_id=self.data_source.id,  # noqa E501
                        )

                    chrom = acc2chrom.get(acc, "")

                    # 2) CREATE VARIANT MASTER OBJECT
                    # (This is the Canonical Variant)
                    variant_master_obj = (
                        self.session.query(VariantMaster)
                        .filter_by(
                            variant_id=variant_master,
                            data_source_id=self.data_source.id,  # noqa: E501
                        )  # noqa: E501
                        .first()
                    )
                    if not variant_master_obj:
                        variant_master_obj = VariantMaster(
                            variant_id=variant_master,
                            entity_id=entity_id,
                            variant_type=(
                                str(row["variant_type"]).upper()
                                if pd.notna(row["variant_type"])
                                else "SNP"
                            ),
                            omic_status_id="1",  # TODO Change it
                            data_source_id=self.data_source.id,
                            assembly_id=int(asm_id),
                            chromosome=str(chrom) if chrom else None,
                            start_pos=(
                                int(row["start_pos"])
                                if pd.notna(row["start_pos"])
                                else None
                            ),
                            end_pos=(
                                int(row["end_pos"])
                                if pd.notna(row["end_pos"])
                                else None
                            ),
                            reference_allele=(
                                str(row["ref"]) if row["ref"] else None
                            ),  # noqa E501
                            alternate_allele=(
                                str(row["alt"]) if row["alt"] else None
                            ),  # noqa E501
                            quality=row["quality"],
                        )

                        self.session.add(variant_master_obj)
                        self.session.flush()  # be sure to variant is generated

                    # TODO:  This tables is under the fisrt Variant Insert,
                    # but next version change to fit with update data
                    # 3) CREATE VARIANT LOCUS TO ALL ASSEMBLIES
                    if variant_master_obj:
                        vlocus_buffer = []
                        seen = (
                            set()
                        )  # dedupe by (variant_id, assembly_id, start, end)  # noqa E501

                        # Canonical
                        canon_acc = str(row["seq_id"])
                        assembly_id = assemblies_map.get(canon_acc)
                        if not assembly_id:
                            self.logger.log(
                                f"⚠️ Unknown assembly accession: {canon_acc} (rs={row['rs_id']})",  # noqa E501
                                "WARNING",
                            )
                        else:
                            start = (
                                int(row["start_pos"])
                                if pd.notna(row["start_pos"])
                                else None
                            )
                            end = (
                                int(row["end_pos"])
                                if pd.notna(row["end_pos"])
                                else None
                            )
                            if start and end:
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
                                            chromosome=acc2chrom.get(
                                                canon_acc
                                            ),  # noqa E501
                                            start_pos=start,
                                            end_pos=end,
                                            data_source_id=self.data_source.id,
                                        )
                                    )

                        # placements (lista de dicts)
                        # TODO: Create a logic to add news Assemblies!
                        placements = row.get("placements") or []
                        for p in placements:
                            p_acc = p.get("seq_id")
                            if not p_acc:
                                continue
                            p_asm = assemblies_map.get(p_acc)
                            if not p_asm:
                                # placements may include uncataloged NG_/NM_/contigs — ignore  # noqa E501
                                continue
                            p_start = p.get("start_pos")
                            p_end = p.get("end_pos")
                            if pd.isna(p_start) or pd.isna(p_end):
                                continue
                            p_start = int(p_start)
                            p_end = int(p_end)

                            key = (
                                variant_master_obj.id,
                                p_asm,
                                p_start,
                                p_end,
                            )  # noqa E501
                            if key in seen:
                                continue
                            seen.add(key)

                            vlocus_buffer.append(
                                VariantLocus(
                                    variant_id=variant_master_obj.id,
                                    assembly_id=p_asm,
                                    chromosome=acc2chrom.get(p_acc),
                                    start_pos=p_start,
                                    end_pos=p_end,
                                    data_source_id=self.data_source.id,
                                )
                            )

                        seen = set()
                        unique_vlocus = []
                        for v in vlocus_buffer:
                            key = (
                                v.variant_id,
                                v.assembly_id,
                                v.chromosome,
                                v.start_pos,
                                v.end_pos,
                            )
                            if key not in seen:
                                seen.add(key)
                                unique_vlocus.append(v)

                        vlocus_buffer = unique_vlocus

                        # insere em lote (sem flush por item)
                        try:
                            if vlocus_buffer:
                                self.session.add_all(vlocus_buffer)
                                self.session.commit()
                                vlocus_buffer.clear()
                        except Exception as e:
                            msg = f"❌ Error to add data em Variants Locus: {str(e)}"  # noqa E501
                            self.logger.log(msg, "ERROR")

                            """ Placement Field Templace
                                [{'alt': 'A', 'assembly': 'GRCh37.p13', 'end_pos': 19813529, 'ref': 'A', 'seq_id': 'NC_000008.10', 'start_pos': 19813529},  # noqa E501
                                {'alt': 'G', 'assembly': 'GRCh37.p13', 'end_pos': 19813529, 'ref': 'A', 'seq_id': 'NC_000008.10', 'start_pos': 19813529},  # noqa E501
                                {'alt': 'A', 'assembly': '', 'end_pos': 59302, 'ref': 'A', 'seq_id': 'NG_008855.2', 'start_pos': 59302},  # noqa E501
                                {'alt': 'G', 'assembly': '', 'end_pos': 59302, 'ref': 'A', 'seq_id': 'NG_008855.2', 'start_pos': 59302}  # noqa E501
                                ]
                            """

                    # 4) CREATE LIST TO ENTITIES LINKS TO GENES (rox)
                    genes_link = row.get("gene_links") or []
                    for g in genes_link:
                        gene_links_rows.append(
                            [variant_master, entity_id, g]
                        )  # rs, rs_entity_id, entrez_id

            except Exception as e:
                msg = f"❌ Error to add data: {str(e)}"
                self.logger.log(msg, "ERROR")

            try:
                # Reade all rows from a file
                if not gene_links_rows:
                    self.logger.log(
                        f"🔗 No variant→gene links in {csv_file}", "INFO"
                    )  # noqa E501
                else:
                    # DF (rs_id, variant_entity_id, entrez_id)
                    df_links = pd.DataFrame(
                        gene_links_rows,
                        columns=["rs_id", "variant_entity_id", "entrez_id"],
                    ).drop_duplicates()

                    # Search GeneMaster from EntrezID
                    entrez_list = (
                        df_links["entrez_id"]
                        .dropna()
                        .astype(int)
                        .unique()
                        .tolist()  # noqa E501
                    )
                    if entrez_list:
                        gm_rows = (
                            self.session.query(
                                GeneMaster.entrez_id, GeneMaster.entity_id
                            )
                            .filter(
                                GeneMaster.entrez_id.in_(
                                    [str(e) for e in entrez_list]
                                )  # noqa E501
                            )  # GeneMaster.entrez_id is String
                            .all()
                        )
                        df_genes = pd.DataFrame(
                            gm_rows,
                            columns=["entrez_id_str", "gene_entity_id"],  # noqa E501
                        )
                        df_genes["entrez_id"] = (
                            df_genes["entrez_id_str"].astype(str).astype(int)
                        )
                        df_genes = df_genes.drop(columns=["entrez_id_str"])
                    else:
                        df_genes = pd.DataFrame(
                            columns=["entrez_id", "gene_entity_id"]
                        )  # noqa E501

                    df_merge = df_links.merge(
                        df_genes, on="entrez_id", how="left"
                    )  # noqa E501

                    found = df_merge[df_merge["gene_entity_id"].notna()].copy()
                    missing = df_merge[
                        df_merge["gene_entity_id"].isna()
                    ].copy()  # noqa E501

                    if not found.empty:
                        found["variant_entity_id"] = found[
                            "variant_entity_id"
                        ].astype(  # noqa E501
                            int
                        )
                        found["gene_entity_id"] = found[
                            "gene_entity_id"
                        ].astype(  # noqa E501
                            int
                        )  # noqa E501

                        found = found.drop_duplicates(
                            subset=["variant_entity_id", "gene_entity_id"]
                        )

                        rel_buffer = []
                        for r in found.itertuples(index=False):
                            rel_buffer.append(
                                EntityRelationship(
                                    entity_1_id=int(r.variant_entity_id),
                                    entity_2_id=int(r.gene_entity_id),
                                    relationship_type_id=rel_type_id,
                                    data_source_id=self.data_source.id,
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
                                    f"⚠️ commit failed inserting relationships ({os.path.basename(csv_file)}): {e}",  # noqa E501
                                    "WARNING",
                                )
                            total += len(chunk)

                        self.logger.log(
                            f"🔗 Inserted {total} EntityRelationship(s) from {os.path.basename(csv_file)}",  # noqa E501
                            "INFO",
                        )
                    else:
                        self.logger.log(
                            f"🔗 No resolvable gene entities in {os.path.basename(csv_file)}",  # noqa E501
                            "INFO",
                        )

                    # Save all genes not found
                    if not missing.empty:
                        missing_file = str(
                            "missing_variant_gene_entities_"
                            + os.path.basename(csv_file).replace(
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

            except IntegrityError as e:
                self.session.rollback()
                total_warnings += 1
                self.logger.log(
                    f"⚠️ Integrity error while loading {os.path.basename(csv_file)}: {e}",  # noqa E501
                    "WARNING",
                )
            except Exception as e:
                self.session.rollback()
                total_warnings += 1
                self.logger.log(
                    f"⚠️ Unexpected error while loading {os.path.basename(csv_file)}: {e}",  # noqa E501
                    "WARNING",
                )

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            self.logger.log(
                f"⚠️ Failed to restore indexes or read-mode: {e}", "WARNING"
            )  # noqa E501

        if total_warnings == 0:
            msg = f"✅ Loaded {total_variants} variants from {len(csv_files)} file(s)."  # noqa E501
            self.logger.log(msg, "SUCCESS")
            return total_variants, True, msg
        else:
            msg = f"Loaded {total_variants} variants with {total_warnings} warning(s). Check logs."  # noqa E501
            self.logger.log(msg, "WARNING")
            return total_variants, True, msg
