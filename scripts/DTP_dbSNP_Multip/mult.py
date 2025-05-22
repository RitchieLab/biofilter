import bz2
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

from scripts.DTP_dbSNP_Multip.worker_utils import (
    process_json_batch,
)  # ✅ função do worker vem de outro módulo


def process_large_json_bz2(
    file_path: Path, output_dir: Path, batch_size=100_000, max_workers=10
):
    futures = []
    batch = []
    batch_id = 0

    with bz2.open(file_path, "rt", encoding="utf-8") as f, ProcessPoolExecutor(
        max_workers=max_workers
    ) as executor:
        for line in f:
            batch.append(line)
            if len(batch) >= batch_size:
                futures.append(
                    executor.submit(
                        process_json_batch, batch.copy(), batch_id, output_dir
                    )
                )
                batch.clear()
                batch_id += 1

        if batch:
            futures.append(
                executor.submit(process_json_batch, batch.copy(), batch_id, output_dir)
            )

        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    input_path = Path(
        "/Users/andrerico/Works/Sys/biofilter/biofilter_data/raw/dbsnp/dbsnp_chry/refsnp-chrY.json.bz2"
    )
    output_dir = Path(
        "/Users/andrerico/Works/Sys/biofilter/biofilter_data/raw/dbsnp/dbsnp_chry/output"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    process_large_json_bz2(input_path, output_dir)
    print("✅ Processing completed.")
