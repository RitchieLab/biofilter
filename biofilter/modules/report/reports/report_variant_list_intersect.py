from __future__ import annotations

import gzip
import re
from pathlib import Path
from typing import Any

import pandas as pd

from biofilter.modules.report.reports.base_report import ReportBase

# ---------------------------------------------------------------------------
# Chromosome normalisation
# ---------------------------------------------------------------------------
_CHR_STR_TO_INT: dict[str, int] = {
    **{str(i): i for i in range(1, 23)},
    "x": 23,
    "y": 24,
    "m": 25,
    "mt": 25,
    "mito": 25,
    "mitochondria": 25,
}


def _chr_to_int(value: Any) -> int | None:
    """Normalise any chromosome representation to biofilter integer encoding."""
    if value is None:
        return None
    s = str(value).strip().lower()
    s = re.sub(r"^chr(?:omosome)?", "", s).strip()
    try:
        return int(s)
    except ValueError:
        return _CHR_STR_TO_INT.get(s)


def _chr_int_to_plink(value: Any) -> str | None:
    """Convert biofilter integer chr to PLINK string (1-22, X, Y, MT)."""
    if value is None:
        return None
    n = int(value)
    if 1 <= n <= 22:
        return str(n)
    if n == 23:
        return "X"
    if n == 24:
        return "Y"
    if n == 25:
        return "MT"
    return str(n)


# ---------------------------------------------------------------------------
# Variant ID parsing helpers
# ---------------------------------------------------------------------------
_RSID_RE = re.compile(r"^rs\d+$", re.IGNORECASE)


def _looks_like_rsid(s: str) -> bool:
    return bool(_RSID_RE.match(s.strip()))


def _parse_chr_pos(s: str) -> tuple[int, int] | None:
    """
    Parse chr:pos from various formats:
      "1:12345", "chr1:12345", "1_12345", "chr1-12345", "1 12345"
    Returns (chr_int, pos_int) or None.
    """
    s = s.strip()
    m = re.match(
        r"^(?:chr(?:omosome)?)?([0-9xymXYM]+)[:\-_ ,\t](\d+)$", s, re.IGNORECASE
    )
    if not m:
        return None
    chr_int = _chr_to_int(m.group(1))
    if chr_int is None:
        return None
    return (chr_int, int(m.group(2)))


def _classify_id(s: str) -> str:
    """Return 'rsid', 'chr_pos', or 'unknown'."""
    if _looks_like_rsid(s):
        return "rsid"
    if _parse_chr_pos(s) is not None:
        return "chr_pos"
    return "unknown"


# ---------------------------------------------------------------------------
# Lista B readers
# ---------------------------------------------------------------------------


def _read_bim(path: Path) -> pd.DataFrame:
    """Read PLINK .bim file → DataFrame with columns: rsid, chr_int, pos."""
    rows = []
    with open(path) as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                parts = line.rstrip("\n").split()
            if len(parts) < 4:
                continue
            chr_int = _chr_to_int(parts[0])
            rsid = parts[1].strip() if _looks_like_rsid(parts[1]) else None
            try:
                pos = int(parts[3])
            except (ValueError, IndexError):
                pos = None
            raw_id = parts[1].strip()
            rows.append(
                {"raw_b_id": raw_id, "rsid": rsid, "chr_int": chr_int, "pos": pos}
            )
    return pd.DataFrame(rows)


def _read_vcf(path: Path) -> pd.DataFrame:
    """Read VCF / VCF.gz → DataFrame with columns: rsid, chr_int, pos."""
    rows = []
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            chr_int = _chr_to_int(parts[0])
            try:
                pos = int(parts[1])
            except ValueError:
                pos = None
            raw_id = parts[2].strip()
            rsid = raw_id if _looks_like_rsid(raw_id) else None
            rows.append(
                {"raw_b_id": raw_id, "rsid": rsid, "chr_int": chr_int, "pos": pos}
            )
    return pd.DataFrame(rows)


def _read_txt(path: Path) -> pd.DataFrame:
    """Read plain text file (one ID per line) → DataFrame."""
    rows = []
    with open(path) as fh:
        for line in fh:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            rsid = raw if _looks_like_rsid(raw) else None
            parsed = _parse_chr_pos(raw)
            chr_int = parsed[0] if parsed else None
            pos = parsed[1] if parsed else None
            rows.append({"raw_b_id": raw, "rsid": rsid, "chr_int": chr_int, "pos": pos})
    return pd.DataFrame(rows)


def _read_csv_b(path: Path, id_col: str | None) -> pd.DataFrame:
    """Read CSV/TSV Lista B → DataFrame."""
    sep = "\t" if str(path).endswith((".tsv", ".bim")) else ","
    df = pd.read_csv(path, sep=sep)
    col = id_col if id_col and id_col in df.columns else df.columns[0]
    rows = []
    for raw in df[col].dropna().astype(str):
        raw = raw.strip()
        rsid = raw if _looks_like_rsid(raw) else None
        parsed = _parse_chr_pos(raw)
        chr_int = parsed[0] if parsed else None
        pos = parsed[1] if parsed else None
        rows.append({"raw_b_id": raw, "rsid": rsid, "chr_int": chr_int, "pos": pos})
    return pd.DataFrame(rows)


def _load_lista_b(path: Path, b_id_col: str | None) -> pd.DataFrame:
    """Dispatch to the right reader based on file extension."""
    ext = "".join(path.suffixes).lower()
    if ext == ".bim":
        return _read_bim(path)
    if ext in (".vcf", ".vcf.gz"):
        return _read_vcf(path)
    if ext in (".txt", ".list", ".snplist"):
        return _read_txt(path)
    # CSV / TSV fallback
    return _read_csv_b(path, b_id_col)


# ---------------------------------------------------------------------------
# Lista A reader
# ---------------------------------------------------------------------------


def _load_lista_a(path: Path, a_id_col: str | None) -> pd.DataFrame:
    """
    Read Lista A from a CSV/TSV file.
    Returns DataFrame with at minimum: variant_a_id, rsid_a, chr_int_a, pos_a.
    All other columns from the original file are preserved.
    """
    sep = "\t" if str(path).endswith(".tsv") else ","
    df = pd.read_csv(path, sep=sep, low_memory=False)

    # Determine the primary ID column
    if a_id_col and a_id_col in df.columns:
        id_col = a_id_col
    else:
        id_col = df.columns[0]

    df = df.rename(columns={id_col: "variant_a_id"})
    df["variant_a_id"] = df["variant_a_id"].astype(str).str.strip()

    # Extract rsid_a from the data
    rsid_candidates = ["rsid", "rs_id", "snp", "snp_id", "variant_id"]
    rsid_col = next((c for c in rsid_candidates if c in df.columns), None)
    if rsid_col and rsid_col != "variant_a_id":
        df["rsid_a"] = df[rsid_col].astype(str).str.strip()
    else:
        df["rsid_a"] = df["variant_a_id"].where(
            df["variant_a_id"].str.match(r"^rs\d+$", na=False)
        )

    # Extract chr_int_a / pos_a
    chr_candidates = ["chromosome", "chr", "chrom"]
    pos_candidates = ["position_start", "position", "pos", "bp"]

    chr_col = next((c for c in chr_candidates if c in df.columns), None)
    pos_col = next((c for c in pos_candidates if c in df.columns), None)

    if chr_col:
        df["chr_int_a"] = df[chr_col].apply(_chr_to_int)
    else:
        df["chr_int_a"] = df["variant_a_id"].apply(
            lambda x: _parse_chr_pos(x)[0] if _parse_chr_pos(x) else None
        )

    if pos_col:
        df["pos_a"] = pd.to_numeric(df[pos_col], errors="coerce").astype("Int64")
    else:
        df["pos_a"] = df["variant_a_id"].apply(
            lambda x: _parse_chr_pos(x)[1] if _parse_chr_pos(x) else None
        )

    return df


# ---------------------------------------------------------------------------
# Main report
# ---------------------------------------------------------------------------


class VariantListIntersectReport(ReportBase):
    name = "variant_list_intersect"
    description = (
        "Intersects a biologically annotated variant list (Lista A) with a "
        "genotyped variant list from VCF/PLINK (Lista B), producing Lista C "
        "— variants present in both. Outputs a full-outer-join DataFrame with "
        "match status and optionally writes a PLINK --extract file."
    )

    @classmethod
    def example_input(cls) -> dict:
        return {
            "variant_list_a": "output/gene_to_variant_filtering.csv",
            "a_id_col": None,
            "variant_list_b": "data/dataset.bim",
            "b_id_col": None,
            "match_by": "auto",
            "plink_extract_path": None,
        }

    @classmethod
    def available_columns(cls) -> list[str]:
        return [
            "variant_a_id",
            "variant_b_id",
            "match_status",
            "plink_id",
        ]

    def run(self) -> pd.DataFrame:
        # ------------------------------------------------------------------ #
        # 1. Parameters
        # ------------------------------------------------------------------ #
        _id = self.param("input_data", default=None)
        if isinstance(_id, dict):
            for k, v in _id.items():
                self.params.setdefault(k, v)

        path_a = self.param("variant_list_a", required=True)
        path_b = self.param("variant_list_b", required=True)
        a_id_col = self.param("a_id_col", default=None)
        b_id_col = self.param("b_id_col", default=None)
        match_by = self.param("match_by", default="auto")
        plink_extract_path = self.param("plink_extract_path", default=None)

        path_a = Path(path_a)
        path_b = Path(path_b)

        if not path_a.exists():
            raise FileNotFoundError(f"variant_list_a not found: {path_a}")
        if not path_b.exists():
            raise FileNotFoundError(f"variant_list_b not found: {path_b}")

        # ------------------------------------------------------------------ #
        # 2. Load both lists
        # ------------------------------------------------------------------ #
        self.logger.log(f"Loading Lista A: {path_a}")
        df_a = _load_lista_a(path_a, a_id_col)
        self.logger.log(f"  → {len(df_a):,} variants")

        self.logger.log(f"Loading Lista B: {path_b}")
        df_b = _load_lista_b(path_b, b_id_col)
        self.logger.log(f"  → {len(df_b):,} variants")

        # ------------------------------------------------------------------ #
        # 3. Determine match strategy
        # ------------------------------------------------------------------ #
        a_has_rsid = df_a["rsid_a"].notna().any()
        b_has_rsid = df_b["rsid"].notna().any()
        a_has_chrpos = df_a["chr_int_a"].notna().any() and df_a["pos_a"].notna().any()
        b_has_chrpos = df_b["chr_int"].notna().any() and df_b["pos"].notna().any()

        use_rsid = False
        use_chrpos = False

        if match_by == "rsid":
            use_rsid = True
        elif match_by == "chr_pos":
            use_chrpos = True
        else:  # auto
            if a_has_rsid and b_has_rsid:
                use_rsid = True
            if a_has_chrpos and b_has_chrpos:
                use_chrpos = True

        self.logger.log(
            f"Match strategy: rsid={use_rsid}, chr_pos={use_chrpos}"
        )

        # ------------------------------------------------------------------ #
        # 4. Build lookup indexes for Lista B
        # ------------------------------------------------------------------ #
        # rsid index: rsid (lowercase) → raw_b_id
        b_rsid_index: dict[str, str] = {}
        if use_rsid:
            for _, row in df_b[df_b["rsid"].notna()].iterrows():
                b_rsid_index[row["rsid"].lower()] = row["raw_b_id"]

        # chr:pos index: (chr_int, pos) → raw_b_id
        b_chrpos_index: dict[tuple[int, int], str] = {}
        if use_chrpos:
            mask = df_b["chr_int"].notna() & df_b["pos"].notna()
            for _, row in df_b[mask].iterrows():
                key = (int(row["chr_int"]), int(row["pos"]))
                b_chrpos_index[key] = row["raw_b_id"]

        # ------------------------------------------------------------------ #
        # 5. Match Lista A against Lista B
        # ------------------------------------------------------------------ #
        variant_b_ids: list[str | None] = []
        match_statuses: list[str] = []
        plink_ids: list[str | None] = []

        for _, row in df_a.iterrows():
            b_id = None
            status = "only_in_a"
            plink_id = None

            # Try rsID match
            if use_rsid and pd.notna(row.get("rsid_a")):
                key = str(row["rsid_a"]).lower()
                if key in b_rsid_index:
                    b_id = b_rsid_index[key]
                    status = "matched_rsid"
                    plink_id = str(row["rsid_a"])

            # Try chr:pos match (only if rsid didn't match)
            if status == "only_in_a" and use_chrpos:
                chr_a = row.get("chr_int_a")
                pos_a = row.get("pos_a")
                if pd.notna(chr_a) and pd.notna(pos_a):
                    key = (int(chr_a), int(pos_a))
                    if key in b_chrpos_index:
                        b_id = b_chrpos_index[key]
                        status = "matched_chr_pos"
                        # Build PLINK-style chr:pos ID
                        plink_chr = _chr_int_to_plink(chr_a)
                        plink_id = f"{plink_chr}:{int(pos_a)}"

            variant_b_ids.append(b_id)
            match_statuses.append(status)
            plink_ids.append(plink_id)

        # ------------------------------------------------------------------ #
        # 6. Assemble result DataFrame
        # ------------------------------------------------------------------ #
        df_a = df_a.copy()
        df_a["variant_b_id"] = variant_b_ids
        df_a["match_status"] = match_statuses
        df_a["plink_id"] = plink_ids

        # Drop internal working columns
        df_a = df_a.drop(
            columns=[c for c in ["rsid_a", "chr_int_a", "pos_a"] if c in df_a.columns]
        )

        # Reorder: identity + match columns first, then original annotation cols
        lead_cols = ["variant_a_id", "variant_b_id", "match_status", "plink_id"]
        other_cols = [c for c in df_a.columns if c not in lead_cols]
        df_result = df_a[lead_cols + other_cols]

        # ------------------------------------------------------------------ #
        # 7. Summary log
        # ------------------------------------------------------------------ #
        n_matched_rsid = (df_result["match_status"] == "matched_rsid").sum()
        n_matched_chrpos = (df_result["match_status"] == "matched_chr_pos").sum()
        n_only_a = (df_result["match_status"] == "only_in_a").sum()
        n_matched = n_matched_rsid + n_matched_chrpos

        self.logger.log(
            f"Results → matched_rsid={n_matched_rsid:,}, "
            f"matched_chr_pos={n_matched_chrpos:,}, "
            f"only_in_a={n_only_a:,} | "
            f"Lista C = {n_matched:,} variants"
        )

        # ------------------------------------------------------------------ #
        # 8. Write PLINK --extract file (Lista C)
        # ------------------------------------------------------------------ #
        if plink_extract_path:
            extract_path = Path(plink_extract_path)
            extract_path.parent.mkdir(parents=True, exist_ok=True)
            matched = df_result[df_result["plink_id"].notna()]["plink_id"]
            matched.to_csv(extract_path, index=False, header=False)
            self.logger.log(
                f"PLINK extract file written: {extract_path} "
                f"({len(matched):,} variants)"
            )

        return df_result
