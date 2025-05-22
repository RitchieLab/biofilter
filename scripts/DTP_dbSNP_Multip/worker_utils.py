import json
import pandas as pd
from pathlib import Path


def process_json_batch(batch, batch_id, output_dir):
    import os

    print(f"[PID {os.getpid()}] Processing batch {batch_id}")

    records = []
    for line in batch:
        try:
            record = json.loads(line)
            rs_id = record.get("refsnp_id")
            placements = record.get("primary_snapshot_data", {}).get(
                "placements_with_allele", []
            )
            for placement in placements:
                if not placement.get("is_ptlp", False):
                    continue
                for allele_info in placement.get("alleles", []):
                    hgvs = allele_info.get("hgvs")
                    if hgvs and ":" in hgvs and "=" not in hgvs:
                        records.append(
                            {
                                "rs_id": rs_id,
                                "hgvs": hgvs,
                            }
                        )
        except Exception:
            continue

    if records:
        df = pd.DataFrame(records)
        part_file = Path(output_dir) / f"processed_part_{batch_id}.csv"
        df.to_csv(part_file, index=False)
