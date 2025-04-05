import apsw
import csv
from pathlib import Path

# Database file and input genes file
base_dir = Path(__file__).parent.parent.parent
DATABASE_FILE = base_dir / "data/loki-20220926.db"
GENES_FILE = (
    base_dir
    / "issues/b15_biofilter_group_annotation/data-in/TEST_ALL_GENES.txt"  # noqa E501
)  # noqa E501


def read_genes(file_path):
    """
    Reads a file containing genes and returns them as a list.
    Assumes each line contains a single gene.
    """
    with open(file_path, "r") as file:
        return [line.strip() for line in file]


def query_genes(database_file, genes):
    """
    Executes the SQL query with the provided list of genes as a filter.
    """
    # Connect to the SQLite database
    connection = apsw.Connection(str(database_file))
    cursor = connection.cursor()

    # Create a temporary table to hold the gene filter
    # Include biopolymer_id from the biopolymer_name table
    cursor.execute(
        """
    CREATE TEMP TABLE main_genes (
        gene_label TEXT,
        biopolymer_id TEXT
    );
    """
    )
    # Insert gene labels and lookup biopolymer_id
    cursor.executemany(
        """
        INSERT INTO main_genes (gene_label, biopolymer_id)
        SELECT ?, biopolymer_id
        FROM biopolymer_name
        WHERE name = ?;
        """,
        [(gene, gene) for gene in genes],
    )

    # Create a temporary table to hold the source filter
    source_ids = {3: "GO", 5: "REACTOME", 7: "KEGG"}
    cursor.execute(
        """
    CREATE TEMP TABLE main_source (
        source_id INTEGER,
        source_label TEXT
    );
    """
    )

    # Insert source filter values
    cursor.executemany(
        "INSERT INTO main_source (source_id, source_label) VALUES (?, ?);",
        source_ids.items(),
    )

    sql_query = """
        SELECT
            filter_biopolymers.biopolymer_id AS gene_id,
            filter_biopolymers.gene_label AS gene_label,
            loki_gb.group_id AS group_id,
            loki_group.label AS group_label,
            loki_gb.source_id AS source_gb_id,
            loki_group.source_id AS source_group_id
        FROM
            main_genes AS filter_biopolymers
        INNER JOIN group_biopolymer AS loki_gb
            ON loki_gb.biopolymer_id = filter_biopolymers.biopolymer_id
        INNER JOIN 'group' AS loki_group
            ON loki_gb.group_id = loki_group.group_id
        INNER JOIN main_source AS filter_source
            ON loki_group.source_id = filter_source.source_id
        WHERE
            loki_gb.specificity >= 100 AND
            loki_gb.biopolymer_id != 0;
        """

    # Execute the query and fetch results
    results = cursor.execute(sql_query).fetchall()

    output_csv = (
        base_dir
        / "issues/b15_biofilter_group_annotation/data-out/results.txt"  # noqa E501
    )  # noqa E501

    # Salvando os resultados no CSV
    with open(output_csv, mode="w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)

        # Escrevendo o cabeçalho do CSV (opcional, ajuste conforme necessário)
        header = [
            "Gene_ID",
            "Gene_code",
            "group_id",
            "source_gb_id",
            "source_group_id",
        ]  # noqa E501
        csv_writer.writerow(header)

        # Escrevendo os dados
        csv_writer.writerows(results)

    print(f"Results saved to {output_csv}")

    # Clean up
    cursor.execute("DROP TABLE main_genes;")
    cursor.execute("DROP TABLE main_source;")
    connection.close()


def main():
    # Read genes from file
    genes = read_genes(GENES_FILE)

    if not genes:
        print("No genes found in the input file.")
        return

    # Execute the query
    query_genes(DATABASE_FILE, genes)


if __name__ == "__main__":
    main()


"""
Query do Biofilter

"SELECT m_bg.label AS gene_label,\n  d_g.label AS group_label,\n  m_c.label AS source_label\n  , (COALESCE(m_bg.biopolymer_id,'')||'_'||COALESCE(d_g.group_id,'')||'_'||COALESCE(m_c.source_id,'')) AS _rowid\nFROM `db`.`group` AS d_g,\n  `db`.`group_biopolymer` AS d_gb,\n  `main`.`gene` AS m_bg,\n  `main`.`source` AS m_c\nWHERE (d_gb.specificity >= 100 OR d_gb.specificity >= 100)\n  AND d_g.group_id = d_gb.group_id\n  AND d_g.source_id = m_c.source_id\n  AND d_gb.biopolymer_id != 0\n  AND d_gb.group_id = d_g.group_id\n  AND m_bg.biopolymer_id = d_gb.biopolymer_id\n  AND m_c.source_id = d_g.source_id\n"
"""
