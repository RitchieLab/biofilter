from __future__ import annotations

import os
import re
import ast
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import func


class ReportBase:
    name: str = "unnamed_report"
    description: str = "No description provided"

    def __init__(self, session=None, db=None, logger=None, **kwargs):
        self.session = session
        self.db = db
        self.logger = logger or self.default_logger()
        self.params = kwargs

        if not self.db:
            raise ValueError("Report requires a Database instance (db=...).")

    # def table(self, name: str):
    #     return self.db.table(name)

    def default_logger(self):
        from biofilter.utils.logger import Logger
        return Logger(name=self.name)

    @classmethod
    def explain(cls) -> str:
        return "No explanation provided."

    @classmethod
    def example_input(cls):
        return None

    def run(self):
        raise NotImplementedError("Subclasses must implement `run()`.")

    # Small helper for params
    def param(self, key: str, default: Any = None, required: bool = False) -> Any:
        if key in self.params:
            return self.params[key]
        if required:
            raise ValueError(f"Missing required parameter: '{key}'")
        return default

    def resolve_input_list(self, input_data, param_name="input_data"):
        """
        Resolves input_data into a list of strings:
        - list[str] → returns as is
        - Path / str path to txt file → loads lines
        - named list → looks into ./input_lists/{name}.txt
        """
        if isinstance(input_data, list):
            return input_data

        if isinstance(input_data, Path):
            input_data = str(input_data)

        if isinstance(input_data, str):
            path = Path(input_data)
            if path.exists():
                with path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            default_path = Path("input_lists") / f"{input_data}.txt"
            if default_path.exists():
                with default_path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            raise FileNotFoundError(f"List file not found: {input_data}")

        raise ValueError(f"{param_name} must be a list or a path to a text file.")

    def resolve_position_list(self, input_data_raw):
        """
        Parses a list or file of genomic positions into (chromosome, position) tuples.

        Supported formats per line:
        - "chr1:1111", "1,1111", "chr2-3333", "4 5555"
        - ("5", 6666)
        - {"chromosome": "1", "position": 123}  (dict input)

        Example:
        resolve_position_list([
            "chr1:1111",
            "1:2222",
            "chr2-3333",
            "chr3,4444",
            "4 5555",
            "chrX;999",
            ("5", 6666),
            "invalid:entry"
        ])
        [('1', 1111), ('1', 2222), ('2', 3333), ('3', 4444), ('4', 5555), ('X', 999), ('5', 6666)]
        """
        
        # Load entries
        if isinstance(input_data_raw, (str, Path)) and os.path.isfile(str(input_data_raw)):
            with open(str(input_data_raw), "r") as f:
                entries = [line.strip() for line in f if line.strip()]
        elif isinstance(input_data_raw, list):
            entries = input_data_raw
        else:
            self.logger.log("Invalid input_data format. Expected list or file path.", "ERROR")
            return []

        positions = []
        for item in entries:
            try:
                if isinstance(item, tuple) and len(item) == 2:
                    chrom, pos = item

                elif isinstance(item, dict):
                    chrom = item.get("chromosome") or item.get("chr")
                    pos = item.get("position") or item.get("pos")
                    if chrom is None or pos is None:
                        raise ValueError("Missing chromosome/position keys")
                    chrom, pos = str(chrom), int(pos)

                elif isinstance(item, str):
                    item_clean = item.lower().replace("chr", "")
                    parts = re.split(r"[:;,\-\s]", item_clean)
                    if len(parts) != 2:
                        raise ValueError("Could not parse chromosome and position")
                    chrom, pos = parts[0].strip(), int(parts[1].strip())

                else:
                    raise ValueError("Unrecognized input format")

                positions.append((str(chrom).upper(), int(pos)))

            except Exception as e:
                self.logger.log(f"⚠️ Skipped malformed input: {item} ({e})", "WARNING")

        return positions
    
    def resolve_assembly_map(self, assembly_input: str) -> dict:
        """
        Resolve an assembly input (e.g., '38', 'GRCh38') to a chromosome → assembly_id map.
        """
        from biofilter.db.models import GenomeAssembly

        assembly_input = str(assembly_input or "").lower()
        if "38" in assembly_input:
            label = "GRCh38.p14"   # TODO: move to config
        elif "37" in assembly_input:
            label = "GRCh37.p13"   # TODO: move to config
        else:
            label = "GRCh38.p14"   # default

        rows = (
            self.session.query(GenomeAssembly.chromosome, GenomeAssembly.id)
            .filter(GenomeAssembly.assembly_name == label)
            .all()
        )
        return {row[0]: row[1] for row in rows}

    def parse_and_join(self, alleles):
        if isinstance(alleles, str):
            try:
                alleles = ast.literal_eval(alleles)
            except (ValueError, SyntaxError):
                return alleles

        if isinstance(alleles, list):
            return "/".join(str(a) for a in alleles)

        return str(alleles) if alleles is not None else None

    # CASE SENSITIVE ****
    def _as_list(self, value: Any) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            return [str(v) for v in value if v is not None and str(v).strip()]
        s = str(value).strip()
        return [s] if s else None

    def _where_in_ci(self, stmt, column, values):
        """
        Case-insensitive WHERE IN for any DB (Postgres/SQLite).
        """
        values_list = self._as_list(values)
        if not values_list:
            return stmt
        values_norm = [v.strip().lower() for v in values_list]
        return stmt.where(func.lower(column).in_(values_norm))
    
    def _filter_ci(self, query, column, values):
        if not values:
            return query

        if isinstance(values, str):
            values = [values]

        values = [v.strip().lower() for v in values if v]
        return query.filter(func.lower(column).in_(values))