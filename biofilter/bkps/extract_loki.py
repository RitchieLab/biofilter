# biofilter/extract/extract_loki.py

from sqlalchemy import create_engine, inspect
import pandas as pd
from biofilter.db.models import (
    Setting,
    GrchUcschg,
    LdProfile,
    Namespace,
    Relationship,
    Role,
    Source,
    SourceOption,
    SourceFile,
    Type,
    Warning,
    SnpMerge,
    SnpLocus,
    SnpEntrezRole,
    SnpBiopolymerRole,
    Biopolymer,
    BiopolymerName,
    BiopolymerNameName,
    BiopolymerRegion,
    BiopolymerZone,
    Group,
    GroupName,
    GroupGroup,
    GroupBiopolymer,
    GroupMemberName,
    Gwas,
    Chain,
    ChainData,
)


def extract_from_loki(loki_path="loki.db", target_path="biofilter.sqlite"):
    loki_engine = create_engine(f"sqlite:///{loki_path}")

    inspector = inspect(loki_engine)
    print(inspector.get_table_names())

    target_engine = create_engine(f"sqlite:///{target_path}")

    tables = {
        "setting": Setting,
        "grch_ucschg": GrchUcschg,
        "ldprofile": LdProfile,
        "namespace": Namespace,
        "relationship": Relationship,
        "role": Role,
        "source": Source,
        "source_option": SourceOption,
        "source_file": SourceFile,
        "type": Type,
        "warning": Warning,
        "snp_merge": SnpMerge,
        "snp_locus": SnpLocus,
        "snp_entrez_role": SnpEntrezRole,
        "snp_biopolymer_role": SnpBiopolymerRole,
        "biopolymer": Biopolymer,
        "biopolymer_name": BiopolymerName,
        "biopolymer_name_name": BiopolymerNameName,
        "biopolymer_region": BiopolymerRegion,
        "biopolymer_zone": BiopolymerZone,
        "group": Group,
        "group_name": GroupName,
        "group_group": GroupGroup,
        "group_biopolymer": GroupBiopolymer,
        "group_member_name": GroupMemberName,
        "gwas": Gwas,
        "chain": Chain,
        "chain_data": ChainData,
    }

    # for table_name, model in tables.items():
    for loki_table_name, model in tables.items():
        print(f"🔄 Copying table: {loki_table_name} ...")

        df = pd.read_sql_table(loki_table_name, con=loki_engine)
        df.to_sql(
            model.__tablename__, con=target_engine, if_exists="append", index=False
        )
        print(f"✅ Done: {len(df)} records copied to {model.__tablename__}")


"""
$ python
>>> from biofilter.extract.extract_loki import extract_from_loki
>>> loki = "/Users/andrerico/Works/Sys/biofilter/data/loki-20220926.db" 
>>> bio = "/Users/andrerico/Works/Sys/biofilter/biofilter/biofilter.sqlite" 
>>> extract_from_loki("loki.db", "biofilter.sqlite")
"""
