# import shutil
# from pathlib import Path
# import pytest


# @pytest.fixture(autouse=True)
# def clean_outcome_folder():
#     """
#     Automatically cleans up the outcome folder before each test run.
#     """
#     outcome_dir = Path(__file__).resolve().parents[0] / "data" / "outcome"

#     if outcome_dir.exists():
#         # Remove all contents but keep the folder itself
#         for item in outcome_dir.iterdir():
#             if item.is_file():
#                 item.unlink()
#             elif item.is_dir():
#                 shutil.rmtree(item)
#     else:
#         outcome_dir.mkdir(parents=True, exist_ok=True)

#     yield  # test executes here

#     # Optional post-cleanup (e.g., remove all generated outputs)
#     for item in outcome_dir.iterdir():
#         if item.is_file():
#             item.unlink()
#         elif item.is_dir():
#             shutil.rmtree(item)
