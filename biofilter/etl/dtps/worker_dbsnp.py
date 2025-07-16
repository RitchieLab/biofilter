import os
import re
import json
import pandas as pd
from pathlib import Path


def worker_dbsnp(batch, batch_id, output_dir, assembly_map):

    print(f"[PID {os.getpid()}] Processing batch {batch_id}")

    results = []

    for line in batch:
        try:
            record = json.loads(line)
            rs_id = f"{record['refsnp_id']}"
            last_build_id = record.get("last_update_build_id", None)
            primary_data = record.get("primary_snapshot_data", {})
            variant_type = primary_data.get("variant_type", None)

            # Save only SNV, but it can be expanded in future versions
            if variant_type != "snv":
                continue

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
                    # match = re.match(r"^(.*?):[gm]\.([\d_]+)(.*)$", hgvs)  # TODO: to process MT  # noqa: E501
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

                    results.append(
                        {
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
                        }
                    )

        except Exception as e:
            print(f"[PID {os.getpid()}] ⚠️ Error in batch {batch_id}: {e}")
            continue

    if results:
        df = pd.DataFrame(results)

        # Map assembly IDs
        df["assembly_id"] = df["seq_id"].map(assembly_map)

        column_order = [
            "build_id",
            "rs_id",
            "seq_id",
            "assembly_id",
            "var_type",
            # "variant_type_id",
            "hgvs",
            "position_base_1",
            "position_start",
            "position_end",
            "allele_type",
            # "allele_type_id",
            "allele",
            "gene_ids",
        ]

        # Reorder the DataFrame columns
        df = df[[col for col in column_order if col in df.columns]]  # noqa: E501
        # part_file = Path(output_dir) / f"processed_part_{batch_id}.csv"
        # df.to_csv(part_file, index=False)

        # Salvar em Parquet em vez de CSV
        parquet_file = Path(output_dir) / f"processed_part_{batch_id}.parquet"
        df.to_parquet(parquet_file, index=False)

        # Worker finished the taks
        print(
            f"[PID {os.getpid()}] ✅ Finished batch {batch_id}, saved {len(df)} rows"  # noqa: E501
        )  # noqa: E501
