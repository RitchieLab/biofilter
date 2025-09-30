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
    
    def resolve_assembly(self, assembly_input: str) -> tuple[int, dict]:
        """
        Normalize and resolve the assembly input to a valid assembly_id from the GenomeAssembly table.

        Parameters:
            assembly_input: str (e.g., '38', 'GRCh38', 'grch38.p14', '37')

        Returns:
            Dict of Chrom : accession_id
        """
        from biofilter.db.models import GenomeAssembly

        if "38" in assembly_input:
            label = "GRCh38.p14"    # TODO: Passar isso para configuracoes
        elif "37" in assembly_input:
            label = "GRCh37.p13"    # TODO: Passar isso para configuracoes
        else:
            raise ValueError(f"Unrecognized assembly input: {assembly_input}")


        # Map chromosome → assembly_id
        rows = (
            self.session.query(GenomeAssembly.chromosome, GenomeAssembly.id)
            .filter(GenomeAssembly.assembly_name == label)
            .all()
        )
        chrom_to_assembly_id = {row[0]: row[1] for row in rows}
        return chrom_to_assembly_id
