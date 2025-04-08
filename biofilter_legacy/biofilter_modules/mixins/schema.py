# schema.py


class Schema:
    """
    A Schema class that defines the database schema for the Biofilter
    application.

    This class serves as a container for the database schema definition,
    allowing the Biofilter class to easily access and use the structure of the
    database tables needed for processing genetic and biological data.

    Attributes:
    ----------
    _schema : dict
        A dictionary representing the database schema. Each key at the top
        level corresponds to a database category ('main', 'user', 'cand'.),
        and within each category, there are specific tables with SQL
        definitions for creating the tables, as well as other properties.

        Example structure:
        {
            'main': {
                'snp': {
                    'table':
                    (
                        rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        label VARCHAR(32) NOT NULL,
                        rs INTEGER NOT NULL,
                        flag TINYINT NOT NULL DEFAULT 0,
                        extra TEXT
                    )
                    ,
                    # Additional table specifications...
                },
                # More tables under 'main'...
            },
            # More categories (e.g., 'alt') with tables...
        }

    Purpose:
    --------
    Encapsulating the schema in this class helps keep the database structure
    organized and separated from the logic within the Biofilter class,promoting
    maintainability and clarity. The schema can be expanded or modified within
    this file without altering the main application code in Biofilter.
    """

    schema = {
        # #################################################
        # main input filter tables (copied for alt)
        "main": {
            "snp": {
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    rs INTEGER NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0,
                    extra TEXT
                    )
                """,
                "index": {
                    "snp__rs": "(rs)",
                },
            },  # main.snp
            "locus": {  # all coordinates in LOKI are 1-based closed intervals
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    chr TINYINT NOT NULL,
                    pos BIGINT NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0,
                    extra TEXT
                    )
                    """,
                "index": {
                    "locus__pos": "(chr,pos)",
                },
            },  # main.locus
            "region": {  # all coordinates in LOKI are 1-based closed intervals
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    chr TINYINT NOT NULL,
                    posMin BIGINT NOT NULL,
                    posMax BIGINT NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0,
                    extra TEXT
                    )
                    """,
                "index": {
                    "region__chr_min": "(chr,posMin)",
                    "region__chr_max": "(chr,posMax)",
                },
            },  # main.region
            "region_zone": {
                "table": """
                    (
                    region_rowid INTEGER NOT NULL,
                    chr TINYINT NOT NULL,
                    zone INTEGER NOT NULL,
                    PRIMARY KEY (chr,zone,region_rowid)
                    )
                    """,
                "index": {
                    "region_zone__region": "(region_rowid)",
                },
            },  # main.region_zone
            "gene": {
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    biopolymer_id INTEGER NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0,
                    extra TEXT
                    )
                    """,
                "index": {
                    "gene__biopolymer": "(biopolymer_id)",
                },
            },  # main.gene
            "group": {
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    group_id INTEGER NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0,
                    extra TEXT
                    )
                    """,
                "index": {
                    "group__group_id": "(group_id)",
                },
            },  # main.group
            "source": {
                "table": """
                    (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    label VARCHAR(32) NOT NULL,
                    source_id INTEGER NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0
                    )
                    """,
                "index": {
                    "source__source_id": "(source_id)",
                },
            },  # main.source
        },  # main
        ##################################################
        # user data tables
        "user": {
            "group": {
                "table": """
                    (
                    group_id INTEGER PRIMARY KEY NOT NULL,
                    label VARCHAR(64) NOT NULL,
                    description VARCHAR(256),
                    source_id INTEGER NOT NULL,
                    extra TEXT
                    )
                    """,
                "index": {
                    "group__label": "(label)",
                },
            },  # user.group
            "group_group": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    related_group_id INTEGER NOT NULL,
                    contains TINYINT,
                    PRIMARY KEY (group_id,related_group_id)
                    )
                    """,
                "index": {
                    "group_group__related": "(related_group_id,group_id)",
                },
            },  # user.group_group
            "group_biopolymer": {
                "table": """
                    (
                    group_id INTEGER NOT NULL,
                    biopolymer_id INTEGER NOT NULL,
                    PRIMARY KEY (group_id,biopolymer_id)
                    )
                    """,
                "index": {
                    "group_biopolymer__biopolymer": "(biopolymer_id,group_id)",
                },
            },  # user.group_biopolymer
            "source": {
                "table": """
                    (
                    source_id INTEGER PRIMARY KEY NOT NULL,
                    source VARCHAR(32) NOT NULL,
                    description VARCHAR(256) NOT NULL
                    )
                    """,
                "index": {},
            },  # user.source
        },  # user
        ##################################################
        # modeling candidate tables
        "cand": {
            "main_biopolymer": {
                "table": """
                    (
                    biopolymer_id INTEGER PRIMARY KEY NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0
                    )
                    """,
                "index": {},
            },  # cand.main_biopolymer
            "alt_biopolymer": {
                "table": """
                    (
                    biopolymer_id INTEGER PRIMARY KEY NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0
                    )
                    """,
                "index": {},
            },  # cand.alt_biopolymer
            "group": {
                "table": """
                    (
                    group_id INTEGER PRIMARY KEY NOT NULL,
                    flag TINYINT NOT NULL DEFAULT 0
                    )
                    """,
                "index": {},
            },  # cand.group
        },  # cand
    }


# _schema{}
