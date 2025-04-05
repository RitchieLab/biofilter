def test_create_gene(db_session):
    from biofilter.db.models.omics_models import Gene

    gene = Gene(
        entity_id=1,
        hgnc_id="HGNC:5",
        symbol="TP53",
        name="tumor protein p53",
        location="17p13.1",
        status="Approved",
    )

    db_session.add(gene)
    db_session.commit()

    result = db_session.query(Gene).filter_by(symbol="TP53").first()
    assert result is not None
    assert result.hgnc_id == "HGNC:5"
