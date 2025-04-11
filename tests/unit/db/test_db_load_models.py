from unittest.mock import patch
from biofilter.utils.db_loader import load_all_models


def test_load_all_models_calls_expected_modules():
    with patch("biofilter.utils.db_loader.import_module") as mock_import:
        load_all_models()

        expected_modules = [
            "biofilter.db.models.config_models",
            "biofilter.db.models.etl_models",
            "biofilter.db.models.entity_models",
            "biofilter.db.models.omics_models",
            "biofilter.db.models.curation_models",
            # "biofilter.db.models.loki_models",
        ]

        actual_calls = [call.args[0] for call in mock_import.call_args_list]
        assert actual_calls == expected_modules
