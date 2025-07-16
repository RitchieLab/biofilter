from biofilter import Biofilter

db_uri = "sqlite:///biofilter.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # Genes
    bf.update(data_sources=["hgnc"], run_steps=["transform"], force_steps=["transform"])  # noqa E501
    bf.update(data_sources=["gene_ncbi"], run_steps=["transform"], force_steps=["transform"])  # noqa E501

    # Proteins
    bf.update(data_sources=["pfam"], run_steps=["transform"], force_steps=["transform"])  # noqa E501
    bf.update(data_sources=["uniprot"], run_steps=["transform"], force_steps=["transform"])  # noqa E501

    # GO
    bf.update(data_sources=["gene_ontology"], run_steps=["transform"], force_steps=["transform"])  # noqa E501

    # Pathways
    bf.update(data_sources=["reactome"], run_steps=["transform"], force_steps=["transform"])  # noqa E501
    bf.update(data_sources=["kegg_pathways"], run_steps=["transform"], force_steps=["transform"])  # noqa E501

    # relationShips (Does not have transform method)
    bf.update(data_sources=["reactome_relationships"], run_steps=["transform"], force_steps=["transform"])  # noqa E501
    bf.update(data_sources=["uniprot_relationships"], run_steps=["transform"], force_steps=["transform"])  # noqa E501
