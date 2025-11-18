# scripts/init_project.py
from biofilter import Biofilter

if __name__ == "__main__":
    import sys

    db_path = sys.argv[1] if len(sys.argv) > 1 else "biofilter.sqlite"
    bf = Biofilter()
    bf.create_new_project(db_path, overwrite=True)


"""
preciso trabalhar mais nesse ponto, a ideia sera criar um exe para iniciar um novo projeto
e criar um novo banco de dados, com os modelos iniciais
"""
