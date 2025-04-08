# #################################################
# INTERNAL QUERY BUILDER MIXIN
# #################################################
import collections
import itertools
import string
import sys


class InternalQueryBuilderMixin:
    """
    Mixin class for constructing and executing SQL queries in a Loki database.

    ATTRIBUTES:
    - _queryAliasTable:
        Dictionary mapping aliases to tables.
    - _queryAliasConditions:
        Dictionary mapping table aliases to conditions.
    - _queryAliasJoinConditions:
        Dictionary mapping pairs of table aliases to join conditions.
    - _queryAliasPairConditions:
        Dictionary mapping pairs of table aliases to conditions.
    - _queryColumnSources:
        Dictionary mapping data columns to table aliases.

    IMPLEMENTED METHODS:
    - [getQueryTemplate]:
        Constructs a template for a SQL query.
    - [buildQuery]:
        Constructs a SQL query from a set of parameters.
    - [getQueryText]:
    - [prepareTablesForQuery]:
        Prepares tables required for executing an SQL query.
    - [generateQueryResults]:
        Executes an SQL query and returns the results.
    """

    # #################################################
    # ATTRIBUTES

    """
    _queryAliasTable:
    A dictionary that maps aliases to tables, streamlining the
    construction of complex SQL queries. Define table aliases for each
    actual table: {alias:(db,table),...}

    This mapping organizes multiple data sources, such as `main`, `alt`,
    `cand`, and `user`, which appear to represent distinct data contexts or
    categories. By using this structure, the system can simplify query
    references, allowing aliases to stand in for full database and table names
    This facilitates building and maintaining complex SQL queries by
    substituting long table identifiers with concise, easily manageable aliases
    """
    _queryAliasTable = {
        "m_s": ("main", "snp"),  # (label,rs)
        "m_l": ("main", "locus"),  # (label,chr,pos)
        "m_r": ("main", "region"),  # (label,chr,posMin,posMax)
        "m_rz": ("main", "region_zone"),  # (region_rowid,chr,zone)
        "m_bg": ("main", "gene"),  # (label,biopolymer_id)
        "m_g": ("main", "group"),  # (label,group_id)
        "m_c": ("main", "source"),  # (label,source_id)
        "a_s": ("alt", "snp"),  # (label,rs)
        "a_l": ("alt", "locus"),  # (label,chr,pos)
        "a_r": ("alt", "region"),  # (label,chr,posMin,posMax)
        "a_rz": ("alt", "region_zone"),  # (region_rowid,chr,zone)
        "a_bg": ("alt", "gene"),  # (label,biopolymer_id)
        "a_g": ("alt", "group"),  # (label,group_id)
        "a_c": ("alt", "source"),  # (label,source_id)
        "c_mb_L": ("cand", "main_biopolymer"),  # (biopolymer_id)
        "c_mb_R": ("cand", "main_biopolymer"),  # (biopolymer_id)
        "c_ab_R": ("cand", "alt_biopolymer"),  # (biopolymer_id)
        "c_g": ("cand", "group"),  # (group_id)
        "u_gb": ("user", "group_biopolymer"),  # (group_id,biopolymer_id)
        "u_gb_L": ("user", "group_biopolymer"),  # (group_id,biopolymer_id)
        "u_gb_R": ("user", "group_biopolymer"),  # (group_id,biopolymer_id)
        "u_g": ("user", "group"),  # (group_id,source_id)
        "u_c": ("user", "source"),  # (source_id)
        "d_sl": ("db", "snp_locus"),  # (rs,chr,pos)
        "d_br": (
            "db",
            "biopolymer_region",
        ),  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
        "d_bz": ("db", "biopolymer_zone"),  # (biopolymer_id,chr,zone)
        "d_b": ("db", "biopolymer"),  # (biopolymer_id,type_id,label)
        "d_gb": (
            "db",
            "group_biopolymer",
        ),  # (group_id,biopolymer_id,specificity,implication,quality)
        "d_gb_L": (
            "db",
            "group_biopolymer",
        ),  # (group_id,biopolymer_id,specificity,implication,quality)
        "d_gb_R": (
            "db",
            "group_biopolymer",
        ),  # (group_id,biopolymer_id,specificity,implication,quality)
        "d_g": ("db", "group"),  # (group_id,type_id,label,source_id)
        "d_c": ("db", "source"),  # (source_id,source)
        "d_w": ("db", "gwas"),  # (rs,chr,pos)
    }  # class._queryAliasTable{}

    """
    _queryAliasConditions:
    Defines constraints for single table aliases.
    dict{ set(a1,a2,...) : set(cond1,cond2,...), ... }

    This dictionary structure maps sets of table aliases (representing
    individual tables) to sets of conditions that apply specifically to those
    aliases, without involving joins or other tables. It is useful for
    applying standalone constraints directly to individual tables within a
    query.
    """
    _queryAliasConditions = {
        # TODO: find a way to put this back here without the covering index
        # problem; hardcoded in buildQuery() for now
        # 	frozenset({'d_sl'}) : frozenset({
        # 		"({allowUSP} OR ({L}.validated > 0))",
        # 	}),
        frozenset({"d_br"}): frozenset(
            {
                "{L}.ldprofile_id = {ldprofileID}",
            }
        ),
        frozenset({"d_gb", "d_gb_L", "d_gb_R"}): frozenset(
            {
                "{L}.biopolymer_id != 0",
                "({L}.{gbColumn1} {gbCondition} OR {L}.{gbColumn2} {gbCondition})",  # noqa E501
            }
        ),
    }  # class._queryAliasConditions{}

    """
    _queryAliasJoinConditions:
    Stores join conditions for pairs of tables, including specific cases,
    such as conditions on expressions that may not be optimized. This
    structure is useful for complex queries where joins require special
    conditions, such as 'chr' and 'pos' for matching regions or genes.

    define constraints for allowable joins of pairs of table aliases:
    dict{ tuple(setL{a1,a2,...},setR{a3,a4,...}) : set{cond1,cond2,...} }
    Note that the SQLite optimizer will not use an index on a column
    which is modified by an expression, even if the condition could
    be rewritten otherwise (i.e. "colA = colB + 10" will not use an
    index on colB).  To account for this, all conditions which include
    expressions must be duplicated so that each operand column appears
    unmodified (i.e. "colA = colB + 10" and also "colA - 10 = colB").
    """
    _queryAliasJoinConditions = {
        (frozenset({"m_s", "a_s", "d_sl"}),): frozenset(
            {
                "{L}.rs = {R}.rs",
            }
        ),
        (frozenset({"m_s", "a_s"}), frozenset({"d_w"})): frozenset(
            {
                "{L}.rs = {R}.rs",
            }
        ),
        (frozenset({"d_sl"}), frozenset({"d_w"})): frozenset(
            {
                "(({L}.rs = {R}.rs) OR ({L}.chr = {R}.chr AND {L}.pos = {R}.pos))",  # noqa E501
            }
        ),
        (frozenset({"m_l", "a_l", "d_sl"}),): frozenset(
            {
                "{L}.chr = {R}.chr",
                "{L}.pos = {R}.pos",
            }
        ),
        (frozenset({"m_l", "a_l"}), frozenset({"d_w"})): frozenset(
            {
                "{L}.chr = {R}.chr",
                "{L}.pos = {R}.pos",
            }
        ),
        (
            frozenset({"m_l", "a_l", "d_sl"}),
            frozenset({"m_rz", "a_rz", "d_bz"}),
        ): frozenset(
            {
                "{L}.chr = {R}.chr",
                "{L}.pos >= (({R}.zone * {zoneSize}) - {rpMargin})",
                "{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rpMargin})",
                "(({L}.pos + {rpMargin}) / {zoneSize}) >= {R}.zone",
                "(({L}.pos - {rpMargin}) / {zoneSize}) <= {R}.zone",
            }
        ),
        (frozenset({"m_rz"}), frozenset({"m_r"})): frozenset(
            {
                "{L}.region_rowid = {R}.rowid",
                # with the rowid match, these should all be guaranteed by self.updateRegionZones()  # noqa E501
                # "{L}.chr = {R}.chr",
                # "(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
                # "({L}.zone * {zoneSize}) <= {R}.posMax",
                # "{L}.zone >= ({R}.posMin / {zoneSize})",
                # "{L}.zone <= ({R}.posMax / {zoneSize})",
            }
        ),
        (frozenset({"a_rz"}), frozenset({"a_r"})): frozenset(
            {
                "{L}.region_rowid = {R}.rowid",
                # with the rowid match, these should all be guaranteed by self.updateRegionZones()  # noqa E501
                # "{L}.chr = {R}.chr",
                # "(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
                # "({L}.zone * {zoneSize}) <= {R}.posMax",
                # "{L}.zone >= ({R}.posMin / {zoneSize})",
                # "{L}.zone <= ({R}.posMax / {zoneSize})",
            }
        ),
        (frozenset({"d_bz"}), frozenset({"d_br"})): frozenset(
            {
                "{L}.biopolymer_id = {R}.biopolymer_id",
                "{L}.chr = {R}.chr",
                # verify the zone/region coverage match in case there are two regions on the same chromosome  # noqa E501
                "(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
                "({L}.zone * {zoneSize}) <= {R}.posMax",
                "{L}.zone >= ({R}.posMin / {zoneSize})",
                "{L}.zone <= ({R}.posMax / {zoneSize})",
            }
        ),
        (frozenset({"m_rz", "a_rz", "d_bz"}),): frozenset(
            {
                "{L}.chr = {R}.chr",
                "{L}.zone >= ({R}.zone + (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",  # noqa E501
                "{L}.zone <= ({R}.zone - (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",  # noqa E501
                "{R}.zone >= ({L}.zone + (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",  # noqa E501
                "{R}.zone <= ({L}.zone - (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",  # noqa E501
            }
        ),
        (frozenset({"m_bg", "a_bg", "d_br", "d_b"}),): frozenset(
            {
                "{L}.biopolymer_id = {R}.biopolymer_id",
            }
        ),
        (
            frozenset({"m_bg", "a_bg", "d_b"}),
            frozenset({"u_gb", "d_gb"}),
        ): frozenset(  # noqa E501
            {
                "{L}.biopolymer_id = {R}.biopolymer_id",
            }
        ),
        (frozenset({"d_gb_L", "d_gb_R"}),): frozenset(
            {
                "{L}.biopolymer_id != {R}.biopolymer_id",
            }
        ),
        (frozenset({"u_gb_L", "u_gb_R"}),): frozenset(
            {
                "{L}.biopolymer_id != {R}.biopolymer_id",
            }
        ),
        (frozenset({"m_g", "a_g", "d_gb", "d_g"}),): frozenset(
            {
                "{L}.group_id = {R}.group_id",
            }
        ),
        (frozenset({"m_g", "a_g", "u_gb", "u_g"}),): frozenset(
            {
                "{L}.group_id = {R}.group_id",
            }
        ),
        (frozenset({"m_c", "a_c", "d_g", "d_c"}),): frozenset(
            {
                "{L}.source_id = {R}.source_id",
            }
        ),
        (frozenset({"m_c", "a_c", "u_g", "u_c"}),): frozenset(
            {
                "{L}.source_id = {R}.source_id",
            }
        ),
        (frozenset({"c_mb_L"}), frozenset({"u_gb_L", "d_gb_L"})): frozenset(
            {
                "{L}.biopolymer_id = {R}.biopolymer_id",
            }
        ),
        (
            frozenset({"c_mb_R", "c_ab_R"}),
            frozenset({"u_gb_R", "d_gb_R"}),
        ): frozenset(  # noqa E501
            {
                "{L}.biopolymer_id = {R}.biopolymer_id",
            }
        ),
        (
            frozenset({"c_g", "d_g"}),
            frozenset({"d_gb", "d_gb_L", "d_gb_R", "d_g"}),
        ): frozenset(
            {
                "{L}.group_id = {R}.group_id",
            }
        ),
        (
            frozenset({"c_g", "u_g"}),
            frozenset({"u_gb", "u_gb_L", "u_gb_R", "u_g"}),
        ): frozenset(
            {
                "{L}.group_id = {R}.group_id",
            }
        ),
    }  # class._queryAliasJoinConditions{}

    """
    _queryAliasPairConditions:
    Defines conditions for pairs of table aliases that are not directly
    connected by a join but still require additional constraints. This is
    useful in cases where two tables share a context-dependent condition,
    even if they are not directly joined.

    The constraints on pairs of table aliases which may not necessarily
    be directly joined; these conditions are added to either the WHERE or the
    LEFT JOIN...ON clause depending on where the aliases appear
    """
    _queryAliasPairConditions = {
        (
            frozenset({"m_l", "a_l", "d_sl"}),
            frozenset({"m_r", "a_r", "d_br"}),
        ): frozenset(
            {
                "{L}.chr = {R}.chr",
                "{L}.pos >= ({R}.posMin - {rpMargin})",
                "{L}.pos <= ({R}.posMax + {rpMargin})",
                "({L}.pos + {rpMargin}) >= {R}.posMin",
                "({L}.pos - {rpMargin}) <= {R}.posMax",
            }
        ),
        (frozenset({"m_r", "a_r", "d_br"}),): frozenset(
            {
                "{L}.chr = {R}.chr",
                "({L}.posMax - {L}.posMin + 1) >= {rmBases}",
                "({R}.posMax - {R}.posMin + 1) >= {rmBases}",
                "("
                + "("
                + "({L}.posMin >= {R}.posMin) AND "
                + "({L}.posMin <= {R}.posMax + 1 - MAX({rmBases}, COALESCE((MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) + 1) * {rmPercent} / 100.0, {rmBases})))"  # noqa E501
                + ") OR ("
                + "({R}.posMin >= {L}.posMin) AND "
                + "({R}.posMin <= {L}.posMax + 1 - MAX({rmBases}, COALESCE((MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) + 1) * {rmPercent} / 100.0, {rmBases})))"  # noqa E501
                + ")"
                + ")",
            }
        ),
    }  # class._queryAliasPairConditions{}

    """
    _queryColumnSources:
    Define available data columns and the table aliases that can provide them,
    in order of preference:
    dict{ col : list[ tuple(alias,rowid,expression,?conditions),... ], ... }
        alias = source alias string
        rowid = source table column which identifies unique results
        "{alias}.{rowid}" must be a valid expression
        expression = full SQL expression for the column (should reference only
            the appropriate alias)
        conditions = optional set of additional conditions

    Maps data sources to each available column, including calculations and
    transformations such as upstream/downstream distances for genes and
    regions. This method provides a useful abstraction for handling various
    data configurations and tables when composing queries, particularly in
    research environments where multiple data sources and representations are
    common.
    """
    _queryColumnSources = {
        "snp_id": [
            ("a_s", "rowid", "a_s.rs"),
            ("m_s", "rowid", "m_s.rs"),
            ("d_sl", "_ROWID_", "d_sl.rs"),
        ],
        "snp_label": [
            ("a_s", "rowid", "a_s.label"),
            ("m_s", "rowid", "m_s.label"),
            ("d_sl", "_ROWID_", "'rs'||d_sl.rs"),
        ],
        "snp_extra": [
            ("a_s", "rowid", "a_s.extra"),
            ("m_s", "rowid", "m_s.extra"),
            ("d_sl", "_ROWID_", "NULL"),
        ],
        "snp_flag": [
            ("a_s", "rowid", "a_s.flag"),
            ("m_s", "rowid", "m_s.flag"),
            ("d_sl", "_ROWID_", "NULL"),
        ],
        "position_id": [
            ("a_l", "rowid", "a_l.rowid"),
            ("m_l", "rowid", "m_l.rowid"),
            ("d_sl", "_ROWID_", "d_sl._ROWID_"),
        ],
        "position_label": [
            ("a_l", "rowid", "a_l.label"),
            ("m_l", "rowid", "m_l.label"),
            ("d_sl", "_ROWID_", "'rs'||d_sl.rs"),
        ],
        "position_chr": [  # TODO: find a way to avoid repeating the conversions already in loki_biofilter.db.chr_name  # noqa E501
            (
                "a_l",
                "rowid",
                "(CASE a_l.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE a_l.chr END)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(CASE m_l.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE m_l.chr END)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(CASE d_sl.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_sl.chr END)",  # noqa E501
            ),
        ],
        "position_pos": [
            ("a_l", "rowid", "a_l.pos {pMinOffset}"),
            ("m_l", "rowid", "m_l.pos {pMinOffset}"),
            ("d_sl", "_ROWID_", "d_sl.pos {pMinOffset}"),
        ],
        "position_extra": [
            ("a_l", "rowid", "a_l.extra"),
            ("m_l", "rowid", "m_l.extra"),
            ("d_sl", "_ROWID_", "NULL"),
        ],
        "position_flag": [
            ("a_l", "rowid", "a_l.flag"),
            ("m_l", "rowid", "m_l.flag"),
            ("d_sl", "_ROWID_", "NULL"),
        ],
        "region_id": [
            ("a_r", "rowid", "a_r.rowid"),
            ("m_r", "rowid", "m_r.rowid"),
            ("d_br", "_ROWID_", "d_br._ROWID_"),
        ],
        "region_label": [
            ("a_r", "rowid", "a_r.label"),
            ("m_r", "rowid", "m_r.label"),
            ("d_b", "biopolymer_id", "d_b.label"),
        ],
        "region_chr": [  # TODO: find a way to avoid repeating the conversions already in loki_biofilter.db.chr_name   # noqa E501
            (
                "a_r",
                "rowid",
                "(CASE a_r.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE a_r.chr END)",  # noqa E501
            ),
            (
                "m_r",
                "rowid",
                "(CASE m_r.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE m_r.chr END)",  # noqa E501
            ),
            (
                "d_br",
                "_ROWID_",
                "(CASE d_br.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_br.chr END)",  # noqa E501
            ),
        ],
        "region_zone": [
            ("a_rz", "zone", "a_rz.zone"),
            ("m_rz", "zone", "m_rz.zone"),
            ("d_bz", "zone", "d_bz.zone"),
        ],
        "region_start": [
            ("a_r", "rowid", "a_r.posMin {pMinOffset}"),
            ("m_r", "rowid", "m_r.posMin {pMinOffset}"),
            ("d_br", "_ROWID_", "d_br.posMin {pMinOffset}"),
        ],
        "region_stop": [
            ("a_r", "rowid", "a_r.posMax {pMaxOffset}"),
            ("m_r", "rowid", "m_r.posMax {pMaxOffset}"),
            ("d_br", "_ROWID_", "d_br.posMax {pMaxOffset}"),
        ],
        "region_extra": [
            ("a_r", "rowid", "a_r.extra"),
            ("m_r", "rowid", "m_r.extra"),
            ("d_br", "_ROWID_", "NULL"),
        ],
        "region_flag": [
            ("a_r", "rowid", "a_r.flag"),
            ("m_r", "rowid", "m_r.flag"),
            ("d_br", "_ROWID_", "NULL"),
        ],
        "biopolymer_id": [
            ("a_bg", "biopolymer_id", "a_bg.biopolymer_id"),
            ("m_bg", "biopolymer_id", "m_bg.biopolymer_id"),
            ("c_mb_L", "biopolymer_id", "c_mb_L.biopolymer_id"),
            ("c_mb_R", "biopolymer_id", "c_mb_R.biopolymer_id"),
            ("c_ab_R", "biopolymer_id", "c_ab_R.biopolymer_id"),
            ("u_gb", "biopolymer_id", "u_gb.biopolymer_id"),
            ("d_br", "biopolymer_id", "d_br.biopolymer_id"),
            ("d_gb", "biopolymer_id", "d_gb.biopolymer_id"),
            ("d_gb_L", "biopolymer_id", "d_gb_L.biopolymer_id"),
            ("d_gb_R", "biopolymer_id", "d_gb_R.biopolymer_id"),
            ("d_b", "biopolymer_id", "d_b.biopolymer_id"),
        ],
        "biopolymer_id_L": [
            ("c_mb_L", "biopolymer_id", "c_mb_L.biopolymer_id"),
            ("u_gb_L", "biopolymer_id", "u_gb_L.biopolymer_id"),
            ("d_gb_L", "biopolymer_id", "d_gb_L.biopolymer_id"),
            ("d_b", "biopolymer_id", "d_b.biopolymer_id"),
        ],
        "biopolymer_id_R": [
            ("c_mb_R", "biopolymer_id", "c_mb_R.biopolymer_id"),
            ("c_ab_R", "biopolymer_id", "c_ab_R.biopolymer_id"),
            ("u_gb_R", "biopolymer_id", "d_gb_R.biopolymer_id"),
            ("d_gb_R", "biopolymer_id", "d_gb_R.biopolymer_id"),
            ("d_b", "biopolymer_id", "d_b.biopolymer_id"),
        ],
        "biopolymer_label": [
            ("a_bg", "biopolymer_id", "a_bg.label"),
            ("m_bg", "biopolymer_id", "m_bg.label"),
            ("d_b", "biopolymer_id", "d_b.label"),
        ],
        "biopolymer_description": [
            ("d_b", "biopolymer_id", "d_b.description"),
        ],
        "biopolymer_identifiers": [
            (
                "a_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = a_bg.biopolymer_id)",  # noqa E501
            ),
            (
                "m_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = m_bg.biopolymer_id)",  # noqa E501
            ),
            (
                "d_b",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = d_b.biopolymer_id)",  # noqa E501
            ),
        ],
        "biopolymer_chr": [  # TODO: find a way to avoid repeating the conversions already in loki_biofilter.db.chr_name  # noqa E501
            (
                "d_br",
                "_ROWID_",
                "(CASE d_br.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_br.chr END)",  # noqa E501
            ),
        ],
        "biopolymer_zone": [
            ("d_bz", "zone", "d_bz.zone"),
        ],
        "biopolymer_start": [
            ("d_br", "_ROWID_", "d_br.posMin {pMinOffset}"),
        ],
        "biopolymer_stop": [
            ("d_br", "_ROWID_", "d_br.posMax {pMaxOffset}"),
        ],
        "biopolymer_extra": [
            ("a_bg", "biopolymer_id", "a_bg.extra"),
            ("m_bg", "biopolymer_id", "m_bg.extra"),
            ("d_b", "biopolymer_id", "NULL"),
        ],
        "biopolymer_flag": [
            ("a_bg", "biopolymer_id", "a_bg.flag"),
            ("m_bg", "biopolymer_id", "m_bg.flag"),
            ("d_b", "biopolymer_id", "NULL"),
        ],
        "gene_id": [
            ("a_bg", "biopolymer_id", "a_bg.biopolymer_id"),
            ("m_bg", "biopolymer_id", "m_bg.biopolymer_id"),
            (
                "d_b",
                "biopolymer_id",
                "d_b.biopolymer_id",
                {"d_b.type_id+0 = {typeID_gene}"},
            ),
        ],
        "gene_label": [
            ("a_bg", "biopolymer_id", "a_bg.label"),
            ("m_bg", "biopolymer_id", "m_bg.label"),
            (
                "d_b",
                "biopolymer_id",
                "d_b.label",
                {"d_b.type_id+0 = {typeID_gene}"},
            ),  # noqa E501
        ],
        "gene_description": [
            (
                "d_b",
                "biopolymer_id",
                "d_b.description",
                {"d_b.type_id+0 = {typeID_gene}"},
            ),
        ],
        "gene_identifiers": [
            (
                "a_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = a_bg.biopolymer_id)",  # noqa E501
            ),
            (
                "m_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = m_bg.biopolymer_id)",  # noqa E501
            ),
            (
                "d_b",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = d_b.biopolymer_id)",  # noqa E501
                {"d_b.type_id+0 = {typeID_gene}"},
            ),
        ],
        "gene_symbols": [
            (
                "a_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = a_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})",  # noqa E501
            ),
            (
                "m_bg",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = m_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})",  # noqa E501
            ),
            (
                "d_b",
                "biopolymer_id",
                "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = d_b.biopolymer_id  AND d_bn.namespace_id = {namespaceID_symbol})",  # noqa E501
                {"d_b.type_id+0 = {typeID_gene}"},
            ),
        ],
        "gene_extra": [
            ("a_bg", "biopolymer_id", "a_bg.extra"),
            ("m_bg", "biopolymer_id", "m_bg.extra"),
            (
                "d_b",
                "biopolymer_id",
                "NULL",
                {"d_b.type_id+0 = {typeID_gene}"},
            ),  # noqa E501
        ],
        "gene_flag": [
            ("a_bg", "biopolymer_id", "a_bg.flag"),
            ("m_bg", "biopolymer_id", "m_bg.flag"),
            (
                "d_b",
                "biopolymer_id",
                "NULL",
                {"d_b.type_id+0 = {typeID_gene}"},
            ),  # noqa E501
        ],
        "upstream_id": [
            (
                "a_l",
                "rowid",
                "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
        ],
        "upstream_label": [
            (
                "a_l",
                "rowid",
                "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
        ],
        "upstream_distance": [
            (
                "a_l",
                "rowid",
                "a_l.pos -(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin})",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "m_l.pos -(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin})",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "d_sl.pos-(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin})",  # noqa E501
            ),
        ],
        "upstream_start": [
            (
                "a_l",
                "rowid",
                "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
        ],
        "upstream_stop": [
            (
                "a_l",
                "rowid",
                "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)",  # noqa E501
            ),
        ],
        "downstream_id": [
            (
                "a_l",
                "rowid",
                "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
        ],
        "downstream_label": [
            (
                "a_l",
                "rowid",
                "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
        ],
        "downstream_distance": [
            (
                "a_l",
                "rowid",
                "-a_l.pos +(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin})",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "-m_l.pos +(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin})",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "-d_sl.pos+(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin})",  # noqa E501
            ),
        ],
        "downstream_start": [
            (
                "a_l",
                "rowid",
                "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
        ],
        "downstream_stop": [
            (
                "a_l",
                "rowid",
                "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "m_l",
                "rowid",
                "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
            (
                "d_sl",
                "_ROWID_",
                "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)",  # noqa E501
            ),
        ],
        "group_id": [
            ("a_g", "group_id", "a_g.group_id"),
            ("m_g", "group_id", "m_g.group_id"),
            ("c_g", "group_id", "c_g.group_id"),
            ("u_gb", "group_id", "u_gb.group_id"),
            ("u_gb_L", "group_id", "u_gb_L.group_id"),
            ("u_gb_R", "group_id", "u_gb_R.group_id"),
            ("u_g", "group_id", "u_g.group_id"),
            ("d_gb", "group_id", "d_gb.group_id"),
            ("d_gb_L", "group_id", "d_gb_L.group_id"),
            ("d_gb_R", "group_id", "d_gb_R.group_id"),
            ("d_g", "group_id", "d_g.group_id"),
        ],
        "group_label": [
            ("a_g", "group_id", "a_g.label"),
            ("m_g", "group_id", "m_g.label"),
            ("u_g", "group_id", "u_g.label"),
            ("d_g", "group_id", "d_g.label"),
        ],
        "group_description": [
            ("u_g", "group_id", "u_g.description"),
            ("d_g", "group_id", "d_g.description"),
        ],
        "group_identifiers": [
            (
                "a_g",
                "group_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = a_g.group_id)",  # noqa E501
            ),
            (
                "m_g",
                "group_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = m_g.group_id)",  # noqa E501
            ),
            ("u_g", "group_id", "u_g.label"),
            (
                "d_g",
                "group_id",
                "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = d_g.group_id)",  # noqa E501
            ),
        ],
        "group_extra": [
            ("a_g", "group_id", "a_g.extra"),
            ("m_g", "group_id", "m_g.extra"),
            ("u_g", "group_id", "NULL"),
            ("d_g", "group_id", "NULL"),
        ],
        "group_flag": [
            ("a_g", "group_id", "a_g.flag"),
            ("m_g", "group_id", "m_g.flag"),
            ("u_g", "group_id", "NULL"),
            ("d_g", "group_id", "NULL"),
        ],
        "source_id": [
            ("a_c", "source_id", "a_c.source_id"),
            ("m_c", "source_id", "m_c.source_id"),
            ("u_g", "source_id", "u_g.source_id"),
            ("u_c", "source_id", "u_c.source_id"),
            ("d_g", "source_id", "d_g.source_id"),
            ("d_c", "source_id", "d_c.source_id"),
        ],
        "source_label": [
            ("a_c", "source_id", "a_c.label"),
            ("m_c", "source_id", "m_c.label"),
            ("u_c", "source_id", "u_c.source"),
            ("d_c", "source_id", "d_c.source"),
        ],
        "gwas_rs": [
            ("d_w", "_ROWID_", "d_w.rs"),
        ],
        "gwas_chr": [
            ("d_w", "_ROWID_", "d_w.chr"),
        ],
        "gwas_pos": [
            ("d_w", "_ROWID_", "d_w.pos {pMinOffset}"),
        ],
        "gwas_trait": [
            ("d_w", "_ROWID_", "d_w.trait"),
        ],
        "gwas_snps": [
            ("d_w", "_ROWID_", "d_w.snps"),
        ],
        "gwas_orbeta": [
            ("d_w", "_ROWID_", "d_w.orbeta"),
        ],
        "gwas_allele95ci": [
            ("d_w", "_ROWID_", "d_w.allele95ci"),
        ],
        "gwas_riskAfreq": [
            ("d_w", "_ROWID_", "d_w.riskAfreq"),
        ],
        "gwas_pubmed": [
            ("d_w", "_ROWID_", "d_w.pubmed_id"),
        ],
    }  # class._queryColumnSources

    # #################################################
    # METHODS

    def getQueryTemplate(self):
        """
        Creates and returns a dictionary template for constructing a SQL query.
        This template organizes query components into standard SQL statement
        sections, allowing modular filling and combining of query elements for
        building dynamic queries.

        Returns:
            dict: A dictionary containing the sections of a SQL query,
            initially empty.

            Dictionary structure:
            - '_columns'  : list()
                List of columns to be selected (example: [colA, colB, ...]).

            - 'SELECT'    : collections.OrderedDict()
                Stores columns and expressions for the SELECT clause, where
                each key is a column alias and the value is the corresponding
                SQL expression.
                Final example: `SELECT expA AS colA, expB AS colB, ...`

            - '_rowid'    : collections.OrderedDict()
                Defines custom row identifiers for specific tables. Each key
                is a table alias, and the value is a set of columns that make
                up the identifier.
                Final example:
                `SELECT ... (tblA.colA1 || '_' || tblA.colA2...) AS rowid`

            - 'FROM'      : set()
                Set of tables to include in the FROM clause.
                Example: `FROM tblA, tblB, ...`

            - 'LEFT JOIN' : collections.OrderedDict()
                Defines tables and conditions for LEFT JOIN operations. Each
                key is a table alias, and the value is a set of join conditions
                for that table. Final example:
                    `LEFT JOIN tblA ON expA1 AND expA2 ...`

            - 'WHERE'     : set()
                Set of expressions for the WHERE clause, applying filters to
                the query. Final example: `WHERE expA AND expB AND ...`

            - 'GROUP BY'  : list()
                List of expressions for the GROUP BY clause, allowing result
                grouping. Final example: `GROUP BY expA, expB, ...`

            - 'HAVING'    : set()
                Set of conditions for the HAVING clause, applied after
                grouping. Final example: `HAVING expA AND expB AND ...`

            - 'ORDER BY'  : list()
                List of expressions for the ORDER BY clause, specifying result
                order. Final example: `ORDER BY expA, expB, ...`

            - 'LIMIT'     : None
                Limit on the number of records returned by the query. Can be
                an integer to cap the total results, e.g., `LIMIT 100`.
        """
        return {
            "_columns": list(),  # [ colA, colB, ... ]
            "SELECT": collections.OrderedDict(),  # {colA:expA, colB:expB, ...}
            # => SELECT expA AS colA, expB AS colB, ...
            "_rowid": collections.OrderedDict(),  # OD{ tblA:{colA1,colA2,...}, ... }  # noqa E501
            # => SELECT ... (tblA.colA1||'_'||tblA.colA2...) AS rowid
            "FROM": set(),  # { tblA, tblB, ... }
            # => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
            "LEFT JOIN": collections.OrderedDict(),  # OD{ tblA:{expA1,expA2,...}, ... }  # noqa E501
            # => LEFT JOIN aliasTable[tblA] ON expA1 AND expA2 ...
            "WHERE": set(),  # { expA, expB, ... }
            # => WHERE expA AND expB AND ...
            "GROUP BY": list(),  # [ expA, expB, ... ]
            # => GROUP BY expA, expB, ...
            "HAVING": set(),  # { expA, expB, ... }
            # => HAVING expA AND expB AND ...
            "ORDER BY": list(),  # [ expA, expB, ... ]
            # => ORDER BY expA, expB, ...
            "LIMIT": None,  # num
            # => LIMIT INT(num)
        }

    def buildQuery(
        self,
        mode,
        focus,
        select,
        having=None,
        where=None,
        applyOffset=False,
        fromFilter=None,
        joinFilter=None,
        userKnowledge=False,
    ):
        """
        Builds a complex SQL query based on specific parameters for filtering,
        joining, and conditions, handling various query modes (like 'filter',
        'annotate', 'modelgene') and applying multiple aliasing rules and
        table filters to generate the final SQL-compatible query based on
        system configurations.

        Parameters:
        - mode (str): Type of query to build ('filter','annotate','modelgene'),
        affecting the tables and conditions included.
        - focus (str): Primary database or set for focusing the query,
        directing tables and conditions.
        - select (list): List of columns to select in the query.
        - having (dict, optional): "Having" conditions applied to selected
        columns, each mapped to a set of operators.
        - where (dict, optional): Conditions for the WHERE clause with
        specified table alias and column.
        - applyOffset (bool): Indicates if a coordinate offset should be
        applied to position columns.
        - fromFilter, joinFilter (dicts, optional): Define inclusion filters
        for tables in `FROM` and `JOIN` based on specific databases
        (e.g., 'main', 'alt', 'cand').
        - userKnowledge (bool): Determines if the user knowledge filter should
        be applied, including relevant tables for specific knowledge.

        Returns:
        - dict: A dictionary representing the query, containing SQL components
        like 'SELECT', 'FROM', 'WHERE', and 'JOIN'.

        Method Steps:
        1. Initialization and Validation:
        - Validates `mode` and `focus` values.
        - Initializes `having` and `where` if not provided, setting default
        filters for tables based on `fromFilter` and `joinFilter`.

        2. Join Graph Generation:
        - Constructs a table alias adjacency graph to map joinable tables,
        based on alias conditions in `_queryAliasJoinConditions`.

        3. Column and Alias Verification:
        - Creates a map of columns and aliases (`columnAliases` and
        `aliasColumns`), checking each requested column's availability.

        4. SELECT Setup:
        - Establishes the column order for `SELECT`, populating `_columns` and
        `SELECT` in the query template.

        5. FROM Clause Construction:
        - Identifies primary table aliases for the `FROM` clause based on
        `fromFilter` and `joinFilter`.

        6. Join Expansion:
        - Completes the `FROM` clause with necessary `LEFT JOIN`s to ensure
        all required tables are connected.

        7. Option Application:
        - Inserts specific system options (e.g., `region_position_margin`,
        `ld_profile`) into alias conditions for SQL formatting.

        8. SELECT and HAVING Conditions:
        - Adds relevant conditions for each column in `SELECT` and `HAVING`,
        based on alias and associated filters.

        9. General Condition Inclusion:
        - Adds general and pair conditions to maintain consistency between
        tables.

        10. Return:
        - Returns the populated `query` dictionary for query execution.

        Exceptions:
        - Raises exceptions if it fails to find suitable tables or aliases for
        requested columns or conditions, or if specified configurations cannot
        be met.

        Notes:
        - This method is highly modular and uses multiple conditionals to
        handle various query modes and focuses, creating an SQL query optimized
        for system needs.
        """
        assert mode in (
            "filter",
            "annotate",
            "modelgene",
            "modelgroup",
            "model",
        )  # noqa E501
        assert focus in self._schema
        # select=[ column, ... ]
        # having={ column:{'= val',...}, ... }
        # where={ (alias,column):{'= val',...}, ... }
        # fromFilter={ db:{table:bool, ...}, ... }
        # joinFilter={ db:{table:bool, ...}, ... }
        if self._options.debug_logic:
            self.warnPush(
                "buildQuery(mode=%s, focus=%s, select=%s, having=%s, where=%s)\n"  # noqa E501
                % (mode, focus, select, having, where)
            )
        having = having or dict()
        where = where or dict()
        if fromFilter is None:
            fromFilter = {
                db: {
                    tbl: bool(flag)
                    for tbl, flag in self._inputFilters[db].items()  # noqa E501
                }  # noqa E501
                for db in ("main", "alt", "cand")
            }
        if joinFilter is None:
            joinFilter = {
                db: {
                    tbl: bool(flag)
                    for tbl, flag in self._inputFilters[db].items()  # noqa E501
                }  # noqa E501
                for db in ("main", "alt", "cand")
            }
        knowFilter = {
            "db": {
                tbl: True
                for db, tbl in iter(self._queryAliasTable.values())
                if (db == "db")
            }
        }
        if userKnowledge:
            knowFilter["user"] = dict()
            for db, tbl in self._queryAliasTable.itervalues():
                if (db == "user") and knowFilter["db"].get(tbl):
                    knowFilter["db"][tbl] = False
                    knowFilter["user"][tbl] = True
        query = self.getQueryTemplate()
        empty = dict()

        # generate table alias join adjacency map
        # (usually this is the entire table join graph, minus nodes that
        # represent empty user input tables, since joining through them would
        # yield zero results by default)
        aliasAdjacent = collections.defaultdict(set)
        for aliasPairs in self._queryAliasJoinConditions:
            for aliasLeft in aliasPairs[0]:
                for aliasRight in aliasPairs[-1]:
                    if aliasLeft != aliasRight:
                        dbLeft, tblLeft = self._queryAliasTable[aliasLeft]
                        dbRight, tblRight = self._queryAliasTable[aliasRight]
                        tblLeft = (
                            "region" if (tblLeft == "region_zone") else tblLeft
                        )  # noqa E501
                        tblRight = (
                            "region"
                            if (tblRight == "region_zone")
                            else tblRight  # noqa E501
                        )  # noqa E501
                        if knowFilter.get(dbLeft, empty).get(
                            tblLeft
                        ) or joinFilter.get(  # noqa E501
                            dbLeft, empty
                        ).get(
                            tblLeft
                        ):
                            if knowFilter.get(dbRight, empty).get(
                                tblRight
                            ) or joinFilter.get(dbRight, empty).get(tblRight):
                                aliasAdjacent[aliasLeft].add(aliasRight)
                                aliasAdjacent[aliasRight].add(aliasLeft)
                            # if aliasRight passes knowledge or join filter
                        # if aliasLeft passes knowledge or join filter
                    # if aliases differ
                # foreach aliasRight
            # foreach aliasLeft
        # foreach _queryAliasJoinConditions

        # debug
        if self._options.debug_logic:
            self.warn("aliasAdjacent = \n")
            for alias in sorted(aliasAdjacent):
                self.warn(
                    "  %s : %s\n" % (alias, sorted(aliasAdjacent[alias]))
                )  # noqa E501

        # generate column availability map
        # _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]  # noqa E501
        columnAliases = collections.defaultdict(list)
        aliasColumns = collections.defaultdict(set)
        for col in itertools.chain(select, having):
            if col not in self._queryColumnSources:
                raise Exception(
                    "internal query with unsupported column '{0}'".format(col)
                )
            if col not in columnAliases:
                for source in self._queryColumnSources[col]:
                    if source[0] in aliasAdjacent:
                        columnAliases[col].append(source[0])
                        aliasColumns[source[0]].add(col)
        if not (columnAliases and aliasColumns):
            raise Exception("internal query with no outputs or conditions")

        # debug
        if self._options.debug_logic:
            self.warn("columnAliases = %s\n" % columnAliases)
            self.warn("aliasColumns = %s\n" % aliasColumns)

        # establish select column order
        for col in select:
            query["_columns"].append(col)
            query["SELECT"][col] = None

        # identify the primary table aliases to query
        # (usually this is all of the user input tables which contain some
        # data, and which match the main/alt focus of this query; since user
        # input represents filters, we always need to join through the tables
        # with that data, even if we're not selecting any of their columns)
        query["FROM"].update(alias for alias, col in where)
        for alias, dbtable in self._queryAliasTable.items():
            db, table = dbtable
            # only include tables which satisfy the filter (usually, user
            # input tables which contain some data)
            if not fromFilter.get(db, empty).get(
                "region" if (table == "region_zone") else table
            ):
                continue
            # only include tables from the focus db (except an alt focus
            # sometimes also includes main)
            if not (
                (db == focus)
                or (
                    db == "main"
                    and focus == "alt"
                    and mode != "annotate"
                    and self._options.alternate_model_filtering != "yes"
                )
            ):
                continue
            # only include tables on one end of the chain when finding
            # candidates for modeling
            if (mode == "modelgene") and (table in ("group", "source")):
                continue
            if (mode == "modelgroup") and (table not in ("group", "source")):
                continue
            # only re-use the main gene candidates on the right if necessary
            if (alias == "c_mb_R") and (
                (self._options.alternate_model_filtering == "yes")
                or fromFilter.get("cand", empty).get("alt_biopolymer")
            ):
                continue
            # otherwise, add it
            query["FROM"].add(alias)
        # foreach table alias

        # if we have no starting point yet, start from the last-resort source
        # for a random output or condition column
        if not query["FROM"]:
            col = next(itertools.chain(select, having))
            for source in self._queryColumnSources[col]:
                db, tbl = self._queryAliasTable[source[0]]
                if knowFilter.get(db, empty).get(tbl):
                    alias = source[0]
            query["FROM"].add(alias)

        # debug
        if self._options.debug_logic:
            self.warn("starting FROM = %s\n" % ", ".join(query["FROM"]))

        # add any table aliases necessary to join the currently included tables
        if len(query["FROM"]) > 1:
            remaining = query["FROM"].copy()
            inside = {remaining.pop()}
            outside = set(aliasAdjacent) - inside
            queue = collections.deque()
            queue.append((inside, outside, remaining))
            while queue:
                inside, outside, remaining = queue.popleft()
                if self._options.debug_logic:
                    self.warn("inside: %s\n" % ", ".join(inside))
                    self.warn("outside: %s\n" % ", ".join(outside))
                    self.warn("remaining: %s\n" % ", ".join(remaining))
                if not remaining:
                    break
                queue.extend(
                    (inside | {a}, outside - {a}, remaining - {a})
                    for a in outside
                    if inside & aliasAdjacent[a]
                )
            if remaining:
                raise Exception(
                    "could not find a join path for starting tables: %s"
                    % query["FROM"]  # noqa E501
                )
            query["FROM"] |= inside
        # if tables need joining

        # debug
        if self._options.debug_logic:
            self.warn("joined FROM = %s\n" % ", ".join(query["FROM"]))

        # add table aliases to satisfy any remaining columns
        columnsRemaining = set(
            col
            for col, aliases in columnAliases.items()
            if not (set(aliases) & query["FROM"])
        )
        if mode == "annotate":
            # when annotating, do a BFS from each remaining column in order of
            # source preference this will guarantee a valid path of LEFT JOINs
            # to the most-preferred available source
            while columnsRemaining:
                target = next(
                    col
                    for col in itertools.chain(select, having)
                    if (col in columnsRemaining)
                )
                if self._options.debug_logic:
                    self.warn("target column = %s\n" % target)
                if not columnAliases[target]:
                    raise Exception(
                        "could not find source table for output column %s"
                        % (target,)  # noqa E501
                    )
                alias = columnAliases[target][0]
                queue = collections.deque()
                queue.append([alias])
                path = None
                while queue:
                    path = queue.popleft()
                    if (path[-1] in query["FROM"]) or (
                        path[-1] in query["LEFT JOIN"]
                    ):  # noqa E501
                        path.pop()
                        break
                    queue.extend(
                        (path + [a])
                        for a in aliasAdjacent[path[-1]]
                        if (a not in path)  # noqa E501
                    )
                    path = None
                if not path:
                    raise Exception(
                        "could not join source table %s for output column %s"
                        % (alias, target)
                    )
                while path:
                    alias = path.pop()
                    columnsRemaining.difference_update(aliasColumns[alias])
                    query["LEFT JOIN"][alias] = set()
                if self._options.debug_logic:
                    self.warn(
                        "new LEFT JOIN = %s\n" % ", ".join(query["LEFT JOIN"])
                    )  # noqa E501
            # while columns need sources
        else:
            # when filtering, build a minimum spanning tree to connect all
            # remaining columns in any order
            # TODO: choose preferred source first as in annotation, rather
            # than blindly expanding until we hit them all?
            if columnsRemaining:
                remaining = columnsRemaining
                inside = query["FROM"]
                outside = set(
                    a
                    for a, t in self._queryAliasTable.items()
                    if (
                        (a not in inside)
                        and (a not in query["LEFT JOIN"])
                        and (
                            knowFilter.get(t[0], empty).get(t[1])
                            or t[1] == "region_zone"
                        )
                    )
                )
                if self._options.debug_logic:
                    self.warn(
                        "remaining columns = %s\n"
                        % ", ".join(columnsRemaining)  # noqa E501
                    )  # noqa E501
                    self.warn("available aliases = %s\n" % ", ".join(outside))
                queue = collections.deque()
                queue.append((inside, outside, remaining))
                while queue:
                    inside, outside, remaining = queue.popleft()
                    if not remaining:
                        break
                    queue.extend(
                        (
                            inside | {a},
                            outside - {a},
                            remaining - aliasColumns[a],
                        )  # noqa E501
                        for a in outside
                        if inside & aliasAdjacent[a]
                    )
                if remaining:
                    raise Exception(
                        "could not find a source table for output columns: %s"
                        % ", ".join(columnsRemaining)
                    )
                query["FROM"] |= inside
            # if columns need sources
        # if annotate

        # debug
        if self._options.debug_logic:
            self.warn("final FROM = %s\n" % ", ".join(query["FROM"]))
            self.warn("final LEFT JOIN = %s\n" % ", ".join(query["LEFT JOIN"]))

        # fetch option values to insert into condition strings
        formatter = string.Formatter()
        options = {
            "L": None,
            "R": None,
            "typeID_gene": self.getOptionTypeID("gene", optional=True),
            "namespaceID_symbol": self.getOptionNamespaceID(
                "symbol", optional=True
            ),  # noqa E501
            "allowUSP": (
                1
                if (self._options.allow_unvalidated_snp_positions == "yes")
                else 0  # noqa E501
            ),
            "pMinOffset": "",
            "pMaxOffset": "",
            "rpMargin": self._options.region_position_margin,
            "rmPercent": (
                self._options.region_match_percent
                if (self._options.region_match_percent is not None)
                else "NULL"
            ),
            "rmBases": (
                self._options.region_match_bases
                if (self._options.region_match_bases is not None)
                else "NULL"
            ),
            "gbColumn1": "specificity",
            "gbColumn2": "specificity",
            "gbCondition": (
                "> 0"
                if (self._options.allow_ambiguous_knowledge == "yes")
                else ">= 100"
            ),
            "zoneSize": int(self._loki.getDatabaseSetting("zone_size") or 0),
            "ldprofileID": self._loki.getLDProfileID(
                self._options.ld_profile or ""
            ),  # noqa E501
        }
        if not options["ldprofileID"]:
            sys.exit(
                "ERROR: %s LD profile record not found in the knowledge database"  # noqa E501
                % (self._options.ld_profile or "<default>",)
            )
        if applyOffset:
            if self._options.coordinate_base != 1:
                options["pMinOffset"] = "+ %d" % (
                    self._options.coordinate_base - 1,
                )  # noqa E501
            if (self._options.coordinate_base != 1) or (
                self._options.regions_half_open == "yes"
            ):
                options["pMaxOffset"] = "+ %d" % (
                    self._options.coordinate_base
                    - 1
                    + (1 if (self._options.regions_half_open == "yes") else 0),
                )
        if self._options.reduce_ambiguous_knowledge == "yes":
            options["gbColumn1"] = (
                "implication"
                if (self._options.reduce_ambiguous_knowledge == "any")
                else self._options.reduce_ambiguous_knowledge
            )
            options["gbColumn2"] = (
                "quality"
                if (self._options.reduce_ambiguous_knowledge == "any")
                else self._options.reduce_ambiguous_knowledge
            )

        # debug
        if self._options.debug_logic:
            self.warn("initial WHERE = %s\n" % query["WHERE"])

        # assign 'select' output columns
        for col in select:
            if query["SELECT"][col] is not None:
                continue
            # _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]  # noqa E501
            for colsrc in self._queryColumnSources[col]:
                if (colsrc[0] in query["FROM"]) or (
                    colsrc[0] in query["LEFT JOIN"]
                ):  # noqa E501
                    if colsrc[0] not in query["_rowid"]:
                        query["_rowid"][colsrc[0]] = set()
                    query["_rowid"][colsrc[0]].add(colsrc[1])
                    query["SELECT"][col] = formatter.vformat(
                        colsrc[2], args=None, kwargs=options
                    )
                    if (len(colsrc) > 3) and colsrc[3]:
                        srcconds = (
                            formatter.vformat(c, args=None, kwargs=options)
                            for c in colsrc[3]
                        )
                        if colsrc[0] in query["FROM"]:
                            query["WHERE"].update(srcconds)
                        elif colsrc[0] in query["LEFT JOIN"]:
                            query["LEFT JOIN"][colsrc[0]].update(srcconds)
                    break
                # if alias is available
            # foreach possible column source
        # foreach output column

        # debug
        if self._options.debug_logic:
            self.warn("SELECT = %s\n" % query["SELECT"])
            self.warn("col WHERE = %s\n" % query["WHERE"])

        # assign 'having' column conditions
        for col, conds in having.items():
            # _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]  # noqa E501
            for colsrc in self._queryColumnSources[col]:
                if (colsrc[0] in query["FROM"]) or (
                    colsrc[0] in query["LEFT JOIN"]
                ):  # noqa E501
                    colconds = (
                        "({0} {1})".format(
                            formatter.vformat(
                                colsrc[2], args=None, kwargs=options
                            ),  # noqa E501
                            c,  # noqa E501
                        )
                        for c in conds
                    )
                    if colsrc[0] in query["FROM"]:
                        query["WHERE"].update(colconds)
                    elif colsrc[0] in query["LEFT JOIN"]:
                        query["LEFT JOIN"][colsrc[0]].update(colconds)

                    if (len(colsrc) > 3) and colsrc[3]:
                        srcconds = (
                            formatter.vformat(c, args=None, kwargs=options)
                            for c in colsrc[3]
                        )
                        if colsrc[0] in query["FROM"]:
                            query["WHERE"].update(srcconds)
                        elif colsrc[0] in query["LEFT JOIN"]:
                            query["LEFT JOIN"][colsrc[0]].update(srcconds)
                    break
                # if alias is available
            # foreach possible column source
        # foreach column condition

        # debug
        if self._options.debug_logic:
            self.warn("having WHERE = %s\n" % query["WHERE"])

        # add 'where' column conditions
        for tblcol, conds in where.items():
            query["WHERE"].update(
                "{0}.{1} {2}".format(
                    tblcol[0],
                    tblcol[1],
                    formatter.vformat(c, args=None, kwargs=options),
                )
                for c in conds
            )

        # debug
        if self._options.debug_logic:
            self.warn("cond WHERE = %s\n" % query["WHERE"])

        # add general constraints for included table aliases
        for aliases, conds in self._queryAliasConditions.items():
            for alias in aliases.intersection(query["FROM"]):
                options["L"] = alias
                query["WHERE"].update(
                    formatter.vformat(c, args=None, kwargs=options)
                    for c in conds  # noqa E501
                )
            for alias in aliases.intersection(query["LEFT JOIN"]):
                options["L"] = alias
                query["LEFT JOIN"][alias].update(
                    formatter.vformat(c, args=None, kwargs=options)
                    for c in conds  # noqa E501
                )

        # TODO: find a way to move this back into _queryAliasConditions
        # without the covering index problem
        if self._options.allow_unvalidated_snp_positions != "yes":
            if "d_sl" in query["FROM"]:
                query["WHERE"].add("d_sl.validated > 0")
            if "d_sl" in query["LEFT JOIN"]:
                query["LEFT JOIN"]["d_sl"].add("d_sl.validated > 0")

        # debug
        if self._options.debug_logic:
            self.warn("table WHERE = %s\n" % query["WHERE"])

        # add join and pair constraints for included table alias pairs
        for aliasPairs, conds in itertools.chain(
            self._queryAliasJoinConditions.items(),
            self._queryAliasPairConditions.items(),
        ):
            for aliasLeft in aliasPairs[0]:
                for aliasRight in aliasPairs[-1]:
                    options["L"] = aliasLeft
                    options["R"] = aliasRight
                    if aliasLeft == aliasRight:
                        pass
                    elif (aliasLeft in query["FROM"]) and (
                        aliasRight in query["FROM"]
                    ):  # noqa E501
                        query["WHERE"].update(
                            formatter.vformat(c, args=None, kwargs=options)
                            for c in conds
                        )
                    elif (aliasLeft in query["FROM"]) and (
                        aliasRight in query["LEFT JOIN"]
                    ):
                        query["LEFT JOIN"][aliasRight].update(
                            formatter.vformat(c, args=None, kwargs=options)
                            for c in conds
                        )
                    elif (aliasLeft in query["LEFT JOIN"]) and (
                        aliasRight in query["FROM"]
                    ):
                        query["LEFT JOIN"][aliasLeft].update(
                            formatter.vformat(c, args=None, kwargs=options)
                            for c in conds
                        )
                    elif (aliasLeft in query["LEFT JOIN"]) and (
                        aliasRight in query["LEFT JOIN"]
                    ):
                        indexLeft = list(query["LEFT JOIN"].keys()).index(
                            aliasLeft
                        )  # noqa E501
                        indexRight = list(query["LEFT JOIN"].keys()).index(
                            aliasRight
                        )  # noqa E501
                        if indexLeft > indexRight:
                            query["LEFT JOIN"][aliasLeft].update(
                                formatter.vformat(c, args=None, kwargs=options)
                                for c in conds
                            )
                        else:
                            query["LEFT JOIN"][aliasRight].update(
                                formatter.vformat(c, args=None, kwargs=options)
                                for c in conds
                            )
                # foreach right alias
            # foreach left alias
        # foreach pair constraint

        # all done
        return query

    def getQueryText(
        self, query, noRowIDs=False, sortRowIDs=False, splitRowIDs=False
    ):  # noqa E501
        """
        Constructs and returns a complete SQL query string based on the
        components of a `query` dictionary. This method uses provided
        configurations and options to include columns, joins, conditions,
        ordering, and other SQL clauses.

        Parameters:
        - query (dict): Dictionary containing the query components, including
        clauses such as `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`,
        `ORDER BY`, and `LIMIT`.
        - noRowIDs (bool): If True, omits the `_rowid` field from the
        selection, which normally forms a concatenated unique identifier.
        - sortRowIDs (bool): If True, adds sorting clauses for `rowIDs`,
        placing null columns at the end.
        - splitRowIDs (bool): If True, adds each row ID column separately,
        named `_rowid_N`, where `N` is the column index.

        Returns:
        - str: Complete SQL string representing the query.

        Method Steps:
        1. **SELECT Clause**:
        - Builds the `SELECT` clause with columns specified in
        `query['_columns']`, adding aliases and default values as "NULL" when
        appropriate.
        - Sets `_rowid` as a concatenated string of row IDs from tables
        specified in `query['_rowid']`, or as separate `_rowid_N` columns if
        `splitRowIDs` is active.

        2. **FROM Clause**:
        - Constructs the `FROM` clause with tables in `query['FROM']`, using
        aliases from `_queryAliasTable` to reference the original databases
        and tables.

        3. **LEFT JOINs**:
        - For each entry in `query['LEFT JOIN']`, adds a `LEFT JOIN` clause
        with specified join conditions.
        - Ensures each `LEFT JOIN` has an appropriate `ON` condition,
        combining multiple conditions with `AND` as needed.

        4. **WHERE Clause**:
        - Adds the `WHERE` clause with all filtering conditions from
        `query['WHERE']`, sorted alphabetically for clarity.

        5. **GROUP BY and HAVING Clauses**:
        - Includes the `GROUP BY` clause to group results based on columns in
        `query['GROUP BY']`.
        - Adds the `HAVING` clause with conditions applicable after grouping,
        listed in alphabetical order.

        6. **ORDER BY Clause**:
        - If `orderBy` is not empty, adds an `ORDER BY` clause with columns
        from `query['ORDER BY']`.
        - Adds `rowIDs` for sorting if `sortRowIDs` is active, placing null
        values at the end.

        7. **LIMIT Clause**:
        - Adds the `LIMIT` clause with the value in `query['LIMIT']`,
        restricting the number of results.

        8. **Return**:
        - Returns the complete SQL string generated from the `query`
        dictionary.

        Output Structure Example:
        - This method produces output that may include `SELECT`, `FROM`,
        `LEFT JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT`,
        depending on the parameters and the `query` dictionary contents.

        Notes:
        - The method provides flexibility for including `rowIDs`, either as
        concatenated or individual columns.
        - It also supports custom conditions and sorting, allowing highly
        specific queries based on system needs and execution parameters.
        """
        sql = (
            "SELECT "
            + (
                ",\n  ".join(
                    "{0} AS {1}".format(query["SELECT"][col] or "NULL", col)
                    for col in query["_columns"]
                )
            )
            + "\n"
        )
        rowIDs = list()
        orderBy = list(query["ORDER BY"])
        for alias, cols in query["_rowid"].items():
            rowIDs.extend(
                "COALESCE({0}.{1},'')".format(alias, col) for col in cols
            )  # noqa E501
            if sortRowIDs:
                orderBy.extend(
                    "({0}.{1} IS NULL)".format(alias, col) for col in cols
                )  # noqa E501
        if splitRowIDs:
            for n in range(len(rowIDs)):
                sql += "  , {0} AS _rowid_{1}\n".format(rowIDs[n], n)
        if not noRowIDs:
            sql += "  , (" + ("||'_'||".join(rowIDs)) + ") AS _rowid\n"
        if query["FROM"]:
            sql += (
                "FROM "
                + (
                    ",\n  ".join(
                        "`{0[0]}`.`{0[1]}` AS {1}".format(
                            self._queryAliasTable[a], a
                        )  # noqa E501
                        for a in sorted(query["FROM"])
                    )
                )
                + "\n"
            )
        for alias, joinon in query["LEFT JOIN"].items():
            sql += "LEFT JOIN `{0[0]}`.`{0[1]}` AS {1}\n".format(
                self._queryAliasTable[alias], alias
            )
            if joinon:
                sql += "  ON " + ("\n  AND ".join(sorted(joinon))) + "\n"
        if query["WHERE"]:
            sql += "WHERE " + ("\n  AND ".join(sorted(query["WHERE"]))) + "\n"
        if query["GROUP BY"]:
            sql += "GROUP BY " + (", ".join(query["GROUP BY"])) + "\n"
        if query["HAVING"]:
            sql += (
                "HAVING " + ("\n  AND ".join(sorted(query["HAVING"]))) + "\n"
            )  # noqa E501
        if orderBy:
            sql += "ORDER BY " + (", ".join(orderBy)) + "\n"
        if query["LIMIT"]:
            sql += "LIMIT " + str(int(query["LIMIT"])) + "\n"
        return sql

    def prepareTablesForQuery(self, query):
        """
        Prepares tables required for executing an SQL query.

        This method iterates over the tables listed in the `FROM` and
        `LEFT JOIN` clauses of the `query` structure and calls
        `prepareTableForQuery` for each relevant table. This ensures that all
        tables are ready for use in the query, applying any necessary
        preparations or checks defined in `prepareTableForQuery`.

        Parameters:
        - query (dict): Dictionary representing the SQL query, containing
        referenced tables in the `FROM` and `LEFT JOIN` clauses.

        Method Steps:
        1. Builds a unique set of `(db, tbl)` pairs for each table alias
            specified in the `FROM` and `LEFT JOIN` clauses of the query.
        2. For each `(db, tbl)` pair, checks if the database `db` and table
            `tbl` exist in the schema (`_schema`).
        3. If both exist, calls `prepareTableForQuery(db, tbl)` to prepare the
            table.

        This method ensures that all tables mentioned in the query are ready
            for use before executing the query.
        """

        for db, tbl in set(
            self._queryAliasTable[a]
            for a in itertools.chain(query["FROM"], query["LEFT JOIN"])
        ):
            if (db in self._schema) and (tbl in self._schema[db]):
                self.prepareTableForQuery(db, tbl)

    def generateQueryResults(
        self, query, allowDupes=False, bindings=None, query2=None
    ):  # noqa E501
        """
        Executes an SQL query and yields the results, optionally allowing
        duplicates.

        This method executes the SQL query generated by `getQueryText()` for
        the provided `query` dictionary and, if specified, an additional
        `query2`. It provides control over duplicate rows in the output and can
        also display query execution plans for debugging purposes.

        Parameters:
        - query (dict): The primary query structure containing clauses and
            parameters.
        - allowDupes (bool): If `True`, allows duplicate rows in the results;
            otherwise, filters duplicates.
        - bindings (list/tuple): Parameter bindings for executing the
            parametrized query.
        - query2 (dict, optional): A secondary query that runs after `query`,
            if provided.

        Behavior:
        - If `debug_query` is enabled, logs the generated SQL and query plan
            for debugging.
        - In normal execution, prepares the tables and runs the queries.
        - When `allowDupes` is `False`, filters duplicates based on the last
            row ID value (last column).
        - If `query2` is provided, executes the second query and combines its
            results with the first result set.

        Returns:
        - Yields each row from the query as a tuple, omitting the row
            identifier when `allowDupes` is `False`.

        Usage Example:
        - `generateQueryResults(query, allowDupes=False, bindings=[val1, val2],
            query2=additional_query)` generates the results of `query` and
            `query2`, removing duplicates.
        """

        # execute the query and yield the results
        cursor = self._loki._biofilter.db.cursor()
        sql = self.getQueryText(query)
        sql2 = self.getQueryText(query2) if query2 else None
        if self._options.debug_query:
            self.log(sql + "\n")
            for row in cursor.execute("EXPLAIN QUERY PLAN " + sql, bindings):
                self.log(str(row) + "\n")
            if query2:
                self.log(sql2 + "\n")
                for row in cursor.execute(
                    "EXPLAIN QUERY PLAN " + sql2, bindings
                ):  # noqa E501
                    self.log(str(row) + "\n")
        else:
            self.prepareTablesForQuery(query)
            if query2:
                self.prepareTablesForQuery(query2)
            if allowDupes:
                lastID = None
                for row in cursor.execute(sql, bindings):
                    if row[-1] != lastID:
                        lastID = row[-1]
                        yield row[:-1]
                if query2:
                    lastID = None
                    for row in cursor.execute(sql2, bindings):
                        if row[-1] != lastID:
                            lastID = row[-1]
                            yield row[:-1]
            else:
                rowIDs = set()
                for row in cursor.execute(sql, bindings):
                    if row[-1] not in rowIDs:
                        rowIDs.add(row[-1])
                        yield row[:-1]
                if query2:
                    for row in cursor.execute(sql2, bindings):
                        if row[-1] not in rowIDs:
                            rowIDs.add(row[-1])
                            yield row[:-1]
                del rowIDs
