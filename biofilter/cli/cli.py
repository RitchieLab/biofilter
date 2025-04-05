# # biofilter/cli/cli.py

# import argparse
# from biofilter.biofilter import Biofilter
# from biofilter.db.database import Database

# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--group", help="Group name to query")
#     args = parser.parse_args()

#     db = Database("sqlite:///biofilter.sqlite")
#     bf = Biofilter(db)

#     result = bf.query_group(name=args.group)
#     for r in result:
#         print(r.name)


# """
# Here we will host the arguments to run the CLI

# in poetry we can add:


# [tool.poetry.scripts]
# biofilter = "biofilter.cli.cli:main""

# """"
