def test_model_gene_to_pathway(biofilter_instance):
    """
    Testa a geração de modelos Gene → Pathway.
    """
    bio = biofilter_instance

    genes = ["BRCA1", "TP53"]
    bio.intersectInputGenes(
        "main",
        bio.generateNamesFromText(genes, "-"),
        errorCallback=lambda *a, **kw: None,
    )

    # Modelo Gene ↔ Pathway
    rows = list(bio.generateModelOutput(["gene"], ["pathway"], applyOffset=True))
    assert len(rows) > 1
    header = rows[0]
    assert any("pathway" in col.lower() for col in header)
