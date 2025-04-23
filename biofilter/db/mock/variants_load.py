import json
from sqlalchemy.orm import Session
from biofilter.db.engine import get_engine
from biofilter.db.base import Base
from biofilter.models.entities import Entity
from biofilter.models.variants_models import (
    GenomeAssembly,
    Variant,
    VariantLocation,
    VariantAnnotation,
)

DEFAULT_ASSEMBLY_NAME = "GRCh38"


def get_or_create_entity(session: Session, entity_type: str) -> Entity:
    entity = Entity(entity_type=entity_type)
    session.add(entity)
    session.flush()  # guarantees `entity.id` is populated
    return entity


def get_default_assembly(session: Session) -> GenomeAssembly:
    assembly = session.query(GenomeAssembly).filter_by(name=DEFAULT_ASSEMBLY_NAME).first()
    if not assembly:
        raise Exception(f"Assembly '{DEFAULT_ASSEMBLY_NAME}' not found.")
    return assembly


def load_variants(session: Session, filepath: str):
    with open(filepath, "r") as f:
        data = json.load(f)

    assembly = get_default_assembly(session)

    for entry in data:
        # Create entity
        entity = get_or_create_entity(session, "variant")

        # Create variant
        variant = Variant(
            entity_id=entity.id,
            rs_id=entry.get("rs_id"),
            variant_type=entry["variant_type"],
            hgvs=entry.get("hgvs"),
            source=entry.get("source"),
            length=entry.get("length"),
        )
        session.add(variant)
        session.flush()

        # Create location (only one, GRCh38)
        loc = entry["location"]
        location = VariantLocation(
            variant_id=variant.id,
            assembly_id=assembly.id,
            chromosome=loc["chromosome"],
            position=loc["position"],
            reference_allele=loc["reference_allele"],
            alternate_allele=loc["alternate_allele"],
        )
        session.add(location)

        # Create annotations (optional)
        for ann in entry.get("annotations", []):
            annotation = VariantAnnotation(
                variant_id=variant.id,
                gene_id=ann.get("gene_id"),
                transcript_id=ann.get("transcript_id"),
                effect=ann.get("effect"),
                clinical_significance=ann.get("clinical_significance"),
                source=ann.get("source"),
                phenotype=ann.get("phenotype"),
                consequence=ann.get("consequence"),
            )
            session.add(annotation)

    session.commit()
    print(f"✔ Loaded {len(data)} variants.")


if __name__ == "__main__":
    engine = get_engine()
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        load_variants(session, "variants.json")
