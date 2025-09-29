from sqlalchemy.orm import Session
from pathlib import Path

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

        if isinstance(input_data, str):
            # Caminho para arquivo
            path = Path(input_data)
            if path.exists():
                with path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            # Nome de lista salva (sem caminho), assumimos ./input_lists/
            default_path = Path("input_lists") / f"{input_data}.txt"
            if default_path.exists():
                with default_path.open() as f:
                    return [line.strip() for line in f if line.strip()]

            raise FileNotFoundError(f"List file not found: {input_data}")

        raise ValueError(f"{param_name} must be a list or a path to a text file.")
    
    def resolve_assembly(self, assembly_input: str, return_mapper: bool = False) -> int | tuple[int, dict]:
        """
        Normalize and resolve the assembly input to a valid assembly_id from the GenomeAssembly table.

        Parameters:
            assembly_input: str (e.g., '38', 'GRCh38', 'grch38.p14', '37')
            return_mapper: if True, also return chromosome → assembly_id map

        Returns:
            int: resolved assembly_id
            OR (int, dict): if return_mapper=True
        """
        from biofilter.db.models import GenomeAssembly

        normalized = str(assembly_input).lower().replace(".", "").replace("p", "").replace("grc", "").replace("h", "")

        if "38" in normalized:
            label = "GRCh38"
        elif "37" in normalized:
            label = "GRCh37"
        else:
            raise ValueError(f"Unrecognized assembly input: {assembly_input}")

        # Fetch the full label from the DB (e.g., GRCh38.p14)
        version = (
            self.session.query(GenomeAssembly.assembly)
            .filter(GenomeAssembly.assembly.ilike(f"{label}%"))
            .limit(1)
            .scalar()
        )

        if not version:
            raise ValueError(f"Genome assembly '{label}' not found in database.")

        # Get one example assembly_id
        assembly_id = (
            self.session.query(GenomeAssembly.id)
            .filter(GenomeAssembly.assembly == version)
            .limit(1)
            .scalar()
        )

        if not assembly_id:
            raise ValueError(f"Could not resolve assembly ID for '{version}'.")

        if return_mapper:
            # Map chromosome → assembly_id
            rows = (
                self.session.query(GenomeAssembly.chromosome, GenomeAssembly.id)
                .filter(GenomeAssembly.assembly == version)
                .all()
            )
            chrom_to_assembly_id = {row[0]: row[1] for row in rows}
            return assembly_id, chrom_to_assembly_id

        return assembly_id
