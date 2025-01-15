# import pytest
from biofilter_modules.mixins.schema import Schema


def test_schema_structure():
    schema = Schema.schema

    # Check top-level keys
    assert "main" in schema
    assert "user" in schema
    assert "cand" in schema

    # Check 'main' category
    main = schema["main"]
    assert "snp" in main
    assert "locus" in main
    assert "region" in main
    assert "region_zone" in main
    assert "gene" in main
    assert "group" in main
    assert "source" in main

    # Check 'user' category
    user = schema["user"]
    assert "group" in user
    assert "group_group" in user
    assert "group_biopolymer" in user
    assert "source" in user

    # Check 'cand' category
    cand = schema["cand"]
    assert "main_biopolymer" in cand
    assert "alt_biopolymer" in cand
    assert "group" in cand


def test_table_definitions():
    schema = Schema.schema

    # Check 'main.snp' table definition
    snp_table = schema["main"]["snp"]["table"].strip()
    assert snp_table.startswith("(")
    assert snp_table.endswith(")")

    # Check 'user.group' table definition
    group_table = schema["user"]["group"]["table"].strip()
    assert group_table.startswith("(")
    assert group_table.endswith(")")

    # Check 'cand.main_biopolymer' table definition
    main_biopolymer_table = schema["cand"]["main_biopolymer"]["table"].strip()
    assert main_biopolymer_table.startswith("(")
    assert main_biopolymer_table.endswith(")")


def test_index_definitions():
    schema = Schema.schema

    # Check 'main.snp' index definition
    snp_index = schema["main"]["snp"]["index"]
    assert "snp__rs" in snp_index
    assert snp_index["snp__rs"] == "(rs)"

    # Check 'user.group' index definition
    group_index = schema["user"]["group"]["index"]
    assert "group__label" in group_index
    assert group_index["group__label"] == "(label)"

    # Check 'cand.main_biopolymer' index definition
    main_biopolymer_index = schema["cand"]["main_biopolymer"]["index"]
    assert main_biopolymer_index == {}
