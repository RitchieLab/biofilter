from sqlalchemy.orm import Session
from pathlib import Path
import os
import re
import ast


class ReportBase:
    name: str = "unnamed_report"
    description: str = "No description provided"

    def __init__(self, session: Session = None, logger=None, **kwargs):
        self.session = session
        # self.logger = logger or self.default_logger()
        self.logger = logger
        self.params = kwargs

    def default_logger(self):
        from biofilter.utils.logger import Logger

        return Logger(name=self.name)

    @classmethod
    def explain(cls) -> str:
        return "No explanation provided."

    @classmethod
    def example_input(cls) -> list[str] | None:
        return None

    def run(self):
        raise NotImplementedError("Subclasses must implement `run()`.")

    def resolve_input_list(self, input_data, param_name="input_data"):
        """
        Resolves the input_data into a list of strings:
        - list → returns as is
        - path to txt file → loads as list
        - named list → looks into default 'input_lists' folder
        """
        if isinstance(input_data, list):
            return input_data

        # TODO: Criar logica para ler da Cloud ou outras fontes
        if isinstance(input_data, str):
            # Caminho para arquivo
            path = Path(input_data)
            if path.exists():
                with path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            # TODO: Preciso melhorar essa logica, criando uma pasta defaul
            # Nome de lista salva (sem caminho), assumimos ./input_lists/
            default_path = Path("input_lists") / f"{input_data}.txt"
            if default_path.exists():
                with default_path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            raise FileNotFoundError(f"List file not found: {input_data}")

        raise ValueError(f"{param_name} must be a list or a path to a text file.")

    # New Functions
    def resolve_position_list(self, input_data_raw):
        """
        Parses a list or file of genomic positions into (chromosome, position) tuples.
        Supports multiple input formats like 'chr1:1111', '1,1111', ('1', 1111), etc.
        """
        # Load input lines
        if isinstance(input_data_raw, str) and os.path.isfile(input_data_raw):
            with open(input_data_raw, "r") as f:
                entries = [line.strip() for line in f if line.strip()]
        elif isinstance(input_data_raw, list):
            entries = input_data_raw
        else:
            self.logger.log(
                "Invalid input_data format. Expected list or file path.", "ERROR"
            )
            return []

        positions = []
        for item in entries:
            try:
                # Case 1: already a tuple
                if isinstance(item, tuple) and len(item) == 2:
                    chrom, pos = item

                # Case 2: string formats
                elif isinstance(item, str):
                    # Remove prefix if present
                    item_clean = item.lower().replace("chr", "")

                    # Split by known separators
                    match = re.split(r"[:;,\-\s]", item_clean)
                    if len(match) != 2:
                        raise ValueError("Could not parse chromosome and position")

                    chrom, pos = match[0].strip(), int(match[1].strip())

                else:
                    raise ValueError("Unrecognized input format")

                positions.append((str(chrom).upper(), int(pos)))

            except Exception as e:
                self.logger.log(f"⚠️ Skipped malformed input: {item} ({e})", "WARNING")
                continue

        return positions

    """
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

    def resolve_assembly(self, assembly_input: str) -> tuple[int, dict]:
        """
        Normalize and resolve the assembly input to a valid assembly_id from the GenomeAssembly table.

        Parameters:
            assembly_input: str (e.g., '38', 'GRCh38', 'grch38.p14', '37')

        Returns:
            Dict of Chrom : accession_id
        """
        from biofilter.db.models import GenomeAssembly

        try:
            if "38" in assembly_input:
                label = "GRCh38.p14"  # TODO: Passar isso para configuracoes
            elif "37" in assembly_input:
                label = "GRCh37.p13"  # TODO: Passar isso para configuracoes
            else:
                # raise ValueError(f"Unrecognized assembly input: {assembly_input}")
                label = "GRCh38.p14"  # TODO: This will be default
        except Exception as e:
            label = "GRCh38.p14"

        # Map chromosome → assembly_id
        rows = (
            self.session.query(GenomeAssembly.chromosome, GenomeAssembly.id)
            .filter(GenomeAssembly.assembly_name == label)
            .all()
        )
        chrom_to_assembly_id = {row[0]: row[1] for row in rows}
        return chrom_to_assembly_id

    # def resolve_assembly(self, assembly_input: str, return_mapper: bool = False) -> int | tuple[int, dict]:
    #     """
    #     Normalize and resolve the assembly input to a valid assembly_id from the GenomeAssembly table.

    #     Parameters:
    #         assembly_input: str (e.g., '38', 'GRCh38', 'grch38.p14', '37')
    #         return_mapper: if True, also return chromosome → assembly_id map

    #     Returns:
    #         int: resolved assembly_id
    #         OR (int, dict): if return_mapper=True
    #     """
    #     from biofilter.db.models import GenomeAssembly

    #     # Normalize input
    #     normalized = str(assembly_input).lower().replace(".", "").replace("p", "").replace("grc", "").replace("h", "")

    #     # Resolve label
    #     if "38" in normalized:
    #         label = "GRCh38.p14"  # TODO: Move to config
    #     elif "37" in normalized:
    #         label = "GRCh37.p13"  # TODO: Move to config
    #     else:
    #         label = "GRCh38.p14"  # Default fallback

    #     # Fetch one example assembly_id
    #     example_assembly_id = (
    #         self.session.query(GenomeAssembly.id)
    #         .filter(GenomeAssembly.assembly_name == label)
    #         .limit(1)
    #         .scalar()
    #     )

    #     if not example_assembly_id:
    #         raise ValueError(f"Genome assembly '{label}' not found in database.")

    #     if return_mapper:
    #         # Map chromosome → assembly_id
    #         rows = (
    #             self.session.query(GenomeAssembly.chromosome, GenomeAssembly.id)
    #             .filter(GenomeAssembly.assembly_name == label)
    #             .all()
    #         )
    #         chrom_to_assembly_id = {row[0]: row[1] for row in rows}
    #         return example_assembly_id, chrom_to_assembly_id

    #     return example_assembly_id
    # """
    #     # Se quiser apenas um ID qualquer
    #     assembly_id = self.resolve_assembly("38")

    #     # Se quiser o mapeamento por cromossomo
    #     _, chrom_to_assembly_id = self.resolve_assembly("38", return_mapper=True)

    #     para criar em setting:
    #     DEFAULT_ASSEMBLY_LABELS = {
    #         "38": "GRCh38.p14",
    #         "37": "GRCh37.p13",
    # }
    # """

    def parse_and_join(self, alleles):
        # Tenta converter de string para lista se necessário
        if isinstance(alleles, str):
            try:
                alleles = ast.literal_eval(alleles)
            except (ValueError, SyntaxError):
                return alleles  # Retorna como está se não for uma lista válida

        if isinstance(alleles, list):
            return "/".join(str(a) for a in alleles)
        return str(alleles) if alleles is not None else None
