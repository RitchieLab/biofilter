# from pathlib import Path
# from unittest.mock import patch
# from biofilter.db.models.config_models import SystemConfig

# # ----------------------------
# # Test settings property
# # ----------------------------

# def test_settings_are_accessible(biofilter_instance):
#     settings = biofilter_instance.settings
#     assert isinstance(settings.get("download_path"), str)


# # ----------------------------
# # Test update()
# # ----------------------------

# def test_update_runs_etl_manager(biofilter_instance):
#     with patch("biofilter.biofilter.ETLManager") as MockETLManager:
#         mock_instance = MockETLManager.return_value

#         biofilter_instance.update(source_system=["TestSystem"])

#         mock_instance.start_process.assert_called_once()
#         args, kwargs = mock_instance.start_process.call_args

#         # Confirma se source_system foi passado corretamente
#         assert kwargs["source_system"] == ["TestSystem"]
#         assert "download_path" in kwargs
#         assert "processed_path" in kwargs


# # ----------------------------
# # Test settings caching
# # ----------------------------

# def test_settings_is_cached(biofilter_instance):
#     first_settings = biofilter_instance.settings
#     second_settings = biofilter_instance.settings
#     assert first_settings is second_settings


# # ----------------------------
# # Test integration: settings + db session
# # ----------------------------

# def test_system_config_is_seeded(biofilter_instance):
#     with biofilter_instance.db.get_session() as session:
#         config_count = session.query(SystemConfig).count()
#         assert config_count > 0


# """
# # Criando os arquivos
# test_full_path = Path("tests/integration/test_biofilter_full.py")
# test_full_path.parent.mkdir(parents=True, exist_ok=True)
# test_full_path.write_text(test_full_code.strip())

# conftest_path = Path("tests/conftest.py")
# conftest_path.parent.mkdir(parents=True, exist_ok=True)
# conftest_path.write_text(conftest_code.strip())

# test_full_path.as_posix()
# """
