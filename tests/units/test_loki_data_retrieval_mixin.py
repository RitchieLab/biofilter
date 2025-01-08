import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.loki_data_retrieval_mixin import (
    LokiDataRetrievalMixin,
)  # noqa: E501


class TestLokiDataRetrievalMixin:
    @pytest.fixture
    def mixin(self):
        mixin = LokiDataRetrievalMixin()
        mixin._loki = MagicMock()
        return mixin

    def test_getSourceFingerprints(self, mixin):
        mixin._loki.getSourceIDs.return_value = {"source1": 1, "source2": 2}
        mixin._loki.getSourceIDVersion.side_effect = lambda x: f"version_{x}"
        mixin._loki.getSourceIDOptions.side_effect = lambda x: f"options_{x}"
        mixin._loki.getSourceIDFiles.side_effect = lambda x: f"files_{x}"

        result = mixin.getSourceFingerprints()
        expected = {
            "source1": ("version_1", "options_1", "files_1"),
            "source2": ("version_2", "options_2", "files_2"),
        }
        assert result == expected

    def test_generateGeneNameStats(self, mixin):
        mixin._loki.getTypeID.return_value = 1
        mixin._loki.generateBiopolymerNameStats.return_value = {
            "gene1": 10,
            "gene2": 20,
        }

        result = mixin.generateGeneNameStats()
        expected = {"gene1": 10, "gene2": 20}
        assert result == expected

    def test_generateGeneNameStats_no_gene_data(self, mixin):
        mixin._loki.getTypeID.return_value = None

        with pytest.raises(SystemExit):
            mixin.generateGeneNameStats()

    def test_generateGroupNameStats(self, mixin):
        mixin._loki.generateGroupNameStats.return_value = {
            "group1": 5,
            "group2": 15,
        }  # noqa: E501

        result = mixin.generateGroupNameStats()
        expected = {"group1": 5, "group2": 15}
        assert result == expected

    def test_generateLDProfiles(self, mixin):
        mixin._loki.getLDProfiles.return_value = {
            "ld1": ("meta1", "data1"),
            "ld2": ("meta2", "data2"),
        }

        result = list(mixin.generateLDProfiles())
        expected = [("ld1", "data1"), ("ld2", "data2")]
        assert result == expected

    def test_getDatabaseGenomeBuilds(self, mixin):
        mixin._loki.getDatabaseSetting.return_value = "19"
        mixin._loki.generateGRChByUCSChg.return_value = ["37", "38"]

        result = mixin.getDatabaseGenomeBuilds()
        expected = (38, 19)
        assert result == expected

    def test_getOptionTypeID(self, mixin):
        mixin._loki.getTypeID.return_value = 1

        result = mixin.getOptionTypeID("type")
        expected = 1
        assert result == expected

    def test_getOptionTypeID_not_found(self, mixin):
        mixin._loki.getTypeID.return_value = None

        with pytest.raises(SystemExit):
            mixin.getOptionTypeID("type")

    def test_getOptionTypeID_optional(self, mixin):
        mixin._loki.getTypeID.return_value = None

        result = mixin.getOptionTypeID("type", optional=True)
        assert result is None

    def test_getOptionNamespaceID(self, mixin):
        mixin._loki.getNamespaceID.return_value = 1

        result = mixin.getOptionNamespaceID("namespace")
        expected = 1
        assert result == expected

    def test_getOptionNamespaceID_primary_label(self, mixin):
        result = mixin.getOptionNamespaceID("-")
        assert result is None

    def test_getOptionNamespaceID_not_found(self, mixin):
        mixin._loki.getNamespaceID.return_value = None

        with pytest.raises(SystemExit):
            mixin.getOptionNamespaceID("namespace")

    def test_getOptionNamespaceID_optional(self, mixin):
        mixin._loki.getNamespaceID.return_value = None

        result = mixin.getOptionNamespaceID("namespace", optional=True)
        assert result is None
