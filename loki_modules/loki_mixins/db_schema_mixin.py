# schema.py


class DbSchemaMixin:
    schema = {
        "db": {
            ##################################################
            # configuration tables
            "setting": {
                "table": """
                    (
                    setting VARCHAR(32) PRIMARY KEY NOT NULL,
                    value VARCHAR(256)
                    )
                """,
                "data": [
                    ("schema", "3"),
                    ("ucschg", None),
                    ("zone_size", "100000"),
                    ("optimized", "0"),
                    ("finalized", "0"),
                ],
                "index": {},
            },  # .biofilter.db.setting
            ##################################################
            # metadata tables
            "grch_ucschg": {
                "table": """
                    (
                    grch INTEGER PRIMARY KEY,
                    ucschg INTEGER NOT NULL
                    )
                    """,
                # translations known at time of writing are still provided,
                # but additional translations will also be fetched at update
                "data": [
                    (34, 16),
                    (35, 17),
                    (36, 18),
                    (37, 19),
                    (38, 38),
                ],
                "index": {},
            },  # .biofilter.db.grch_ucschg
            "ldprofile": {
                "table": """
                    (
                    ldprofile_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    ldprofile VARCHAR(32) UNIQUE NOT NULL,
                    description VARCHAR(128),
                    metric VARCHAR(32),
                    value DOUBLE
                    )
                    """,
                "index": {},
            },  # .biofilter.db.ldprofile
            "namespace": {
                "table": """
                    (
                    namespace_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    namespace VARCHAR(32) UNIQUE NOT NULL,
                    polygenic TINYINT NOT NULL DEFAULT 0
                    )
                    """,
                "index": {},
            },  # .biofilter.db.namespace
            "relationship": {
                "table": """
                    (
                    relationship_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    relationship VARCHAR(32) UNIQUE NOT NULL
                    )
                    """,
                "index": {},
            },  # .biofilter.db.relationship
            "role": {
                "table": """
                    (
                    role_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    role VARCHAR(32) UNIQUE NOT NULL,
                    description VARCHAR(128),
                    coding TINYINT,
                    exon TINYINT
                    )
                    """,
                "index": {},
            },  # .biofilter.db.role
            "source": {
                "table": """
                    (
                    source_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    source VARCHAR(32) UNIQUE NOT NULL,
                    updated DATETIME,
                    version VARCHAR(32),
                    grch INTEGER,
                    ucschg INTEGER,
                    current_ucschg INTEGER,
                    last_status BOOLEAN DEFAULT 0
                    )
                    """,
                "index": {},
            },  # .biofilter.db.source
            "source_option": {
                "table": """
                    (
                    source_id TINYINT NOT NULL,
                    option VARCHAR(32) NOT NULL,
                    value VARCHAR(64),
                    PRIMARY KEY (source_id, option)
                    )
                    """,
                "index": {},
            },  # .biofilter.db.source_option
            "source_file": {
                "table": """
                    (
                    source_id TINYINT NOT NULL,
                    filename VARCHAR(256) NOT NULL,
                    size BIGINT,
                    modified DATETIME,
                    md5 VARCHAR(64),
                    PRIMARY KEY (source_id, filename)
                    )
                    """,
                "index": {},
            },  # .biofilter.db.source_file
            "type": {
                "table": """
                    (
                    type_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    type VARCHAR(32) UNIQUE NOT NULL
                    )
                    """,
                "index": {},
            },  # .biofilter.db.type
            # # NOTE: NEW TABLE
            # "subtype": {
            #     "table": """
            #         (
            #         subtype_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            #         subtype VARCHAR(32) UNIQUE NOT NULL
            #         )
            #         """,
            #     "index": {},
            # },  # .biofilter.db.subtype
            "warning": {
                "table": """
                    (
                    warning_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    source_id TINYINT NOT NULL,
                    warning VARCHAR(8192)
                    )
                    """,
                "index": {
                    "warning__source": "(source_id)",
                },
            },  # .biofilter.db.warning
            ##################################################
            # snp tables
            # 🚧 TODO Revisar se vamos manter indice composto ou simples.
            "snp_merge": {
                "table": """
                    (
                    rsMerged INTEGER NOT NULL,
                    rsCurrent INTEGER NOT NULL,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "snp_merge__merge_current": "(rsMerged,rsCurrent)",
                },
            },  # .biofilter.db.snp_merge
            "snp_locus": {  # all coord in LOKI are 1-based closed intervals
                "table": """
                    (
                    rs INTEGER NOT NULL,
                    chr TINYINT NOT NULL,
                    pos BIGINT NOT NULL,
                    validated TINYINT NOT NULL,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "snp_locus__rs_chr_pos": "(rs,chr,pos)",
                    "snp_locus__chr_pos_rs": "(chr,pos,rs)",
                    # a (validated,...) idx would be nice but adds >1GB to the
                    # file size 'snp_locus__valid_chr_pos_rs':
                    # '(validated,chr,pos,rs)',
                },
            },  # .biofilter.db.snp_locus
            "snp_entrez_role": {
                "table": """
                    (
                    rs INTEGER NOT NULL,
                    entrez_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "snp_entrez_role__rs_entrez_role": "(rs,entrez_id,role_id)",  # noqa E501
                },
            },  # .biofilter.db.snp_entrez_role
            "snp_biopolymer_role": {
                "table": """
                    (
                    rs INTEGER NOT NULL,
                    biopolymer_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "snp_biopolymer_role__rs_biopolymer_role": "(rs,biopolymer_id,role_id)",  # noqa E501
                    "snp_biopolymer_role__biopolymer_rs_role": "(biopolymer_id,rs,role_id)",  # noqa E501
                },
            },  # .biofilter.db.snp_biopolymer_role
            ##################################################
            # biopolymer tables
            "biopolymer": {
                "table": """
                    (
                    biopolymer_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    type_id TINYINT NOT NULL,
                    label VARCHAR(64) NOT NULL,
                    description VARCHAR(256),
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "biopolymer__type": "(type_id)",
                    "biopolymer__label_type": "(label,type_id)",
                },
            },  # .biofilter.db.biopolymer
            "biopolymer_name": {
                "table": """
                    (
                    biopolymer_id INTEGER NOT NULL,
                    namespace_id INTEGER NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (biopolymer_id,namespace_id,name)
                    )
                    """,
                "index": {
                    "biopolymer_name__name_namespace_biopolymer": "(name,namespace_id,biopolymer_id)",  # noqa E501
                },
            },  # .biofilter.db.biopolymer_name
            "biopolymer_name_name": {
                # PRIMARY KEY column order satisfies the need to GROUP BY new_namespace_id, new_name  # noqa E501
                "table": """
                    (
                    namespace_id INTEGER NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    type_id TINYINT NOT NULL,
                    new_namespace_id INTEGER NOT NULL,
                    new_name VARCHAR(256) NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (new_namespace_id,new_name,type_id,namespace_id,name)
                    )
                    """,
                "index": {},
            },  # .biofilter.db.biopolymer_name_name
            "biopolymer_region": {  # all coordinates in LOKI are 1-based closed intervals  # noqa E501
                "table": """
                    (
                    biopolymer_id INTEGER NOT NULL,
                    ldprofile_id INTEGER NOT NULL,
                    chr TINYINT NOT NULL,
                    posMin BIGINT NOT NULL,
                    posMax BIGINT NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (biopolymer_id,ldprofile_id,chr,posMin,posMax)
                    )
                    """,
                "index": {
                    "biopolymer_region__ldprofile_chr_min": "(ldprofile_id,chr,posMin)",  # noqa E501
                    "biopolymer_region__ldprofile_chr_max": "(ldprofile_id,chr,posMax)",  # noqa E501
                },
            },  # .biofilter.db.biopolymer_region
            "biopolymer_zone": {
                "table": """
                    (
                    biopolymer_id INTEGER NOT NULL,
                    chr TINYINT NOT NULL,
                    zone INTEGER NOT NULL,
                    PRIMARY KEY (biopolymer_id,chr,zone)
                    )
                    """,
                "index": {
                    "biopolymer_zone__zone": "(chr,zone,biopolymer_id)",
                },
            },  # .biofilter.db.biopolymer_zone
            ##################################################
            # group tables
            "group": {
                "table": """
                    (
                    group_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    type_id TINYINT NOT NULL,
                    label VARCHAR(64) NOT NULL,
                    description VARCHAR(256),
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "group__type": "(type_id)",
                    "group__label_type": "(label,type_id)",
                },
            },  # .biofilter.db.group
            "group_name": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    namespace_id INTEGER NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (group_id,namespace_id,name)
                    )
                    """,
                "index": {
                    "group_name__name_namespace_group": "(name,namespace_id,group_id)",  # noqa E501
                    "group_name__source_name": "(source_id,name)",
                },
            },  # .biofilter.db.group_name
            "group_group": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    related_group_id INTEGER NOT NULL,
                    relationship_id SMALLINT NOT NULL,
                    direction TINYINT NOT NULL,
                    contains TINYINT,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (group_id,related_group_id,relationship_id,direction)
                    )
                    """,
                "index": {
                    "group_group__related": "(related_group_id,group_id)",
                },
            },  # .biofilter.db.group_group
            "group_biopolymer": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    biopolymer_id INTEGER NOT NULL,
                    specificity TINYINT NOT NULL,
                    implication TINYINT NOT NULL,
                    quality TINYINT NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (group_id,biopolymer_id,source_id)
                    )
                    """,
                "index": {
                    "group_biopolymer__biopolymer": "(biopolymer_id,group_id)",
                },
            },  # .biofilter.db.group_biopolymer
            "group_member_name": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    member INTEGER NOT NULL,
                    type_id TINYINT NOT NULL,
                    namespace_id INTEGER NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (group_id,member,type_id,namespace_id,name)
                    )
                    """,
                "index": {},
            },  # .biofilter.db.group_member_name
            ##################################################
            # gwas tables
            "gwas": {  # all coordinates in LOKI are 1-based closed intervals
                "table": """
                    (
                    gwas_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    rs INTEGER,
                    chr TINYINT,
                    pos BIGINT,
                    trait VARCHAR(256) NOT NULL,
                    snps VARCHAR(256),
                    orbeta VARCHAR(8),
                    allele95ci VARCHAR(16),
                    riskAfreq VARCAHR(16),
                    pubmed_id INTEGER,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "gwas__rs": "(rs)",
                    "gwas__chr_pos": "(chr,pos)",
                },
            },  # .biofilter.db.gwas
            ##################################################
            # liftover tables
            "chain": {  # all coordinates in LOKI are 1-based closed intervals
                "table": """
                    (
                    chain_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    old_ucschg INTEGER NOT NULL,
                    old_chr TINYINT NOT NULL,
                    old_start BIGINT NOT NULL,
                    old_end BIGINT NOT NULL,
                    new_ucschg INTEGER NOT NULL,
                    new_chr TINYINT NOT NULL,
                    new_start BIGINT NOT NULL,
                    new_end BIGINT NOT NULL,
                    score BIGINT NOT NULL,
                    is_fwd TINYINT NOT NULL,
                    source_id TINYINT NOT NULL
                    )
                    """,
                "index": {
                    "chain__oldhg_newhg_chr": "(old_ucschg,new_ucschg,old_chr)",  # noqa E501
                },
            },  # .biofilter.db.chain
            "chain_data": {  # all coordi in LOKI are 1-based closed intervals
                "table": """
                    (
                    chain_id INTEGER NOT NULL,
                    old_start BIGINT NOT NULL,
                    old_end BIGINT NOT NULL,
                    new_start BIGINT NOT NULL,
                    source_id TINYINT NOT NULL,
                    PRIMARY KEY (chain_id,old_start)
                    )
                    """,
                "index": {
                    "chain_data__end": "(chain_id,old_end)",
                },
            },  # .biofilter.db.chain_data
        },  # .db
    }  # _schema{}
