# core/mixins/updater_mixin.py


class UpdaterMixin:
    def update(self, overwrite=False):

        self._download_sources()
        self._extract_files()
        self._transform_data()
        self._load_to_database(overwrite=overwrite)

    def _download_sources(self):
        # download NCBI, KEGG, PharmGKB, etc.
        pass

    def _extract_files(self):
        # descompacta, normaliza formatos
        pass

    def _transform_data(self):
        # aplica schema interno, converte para DataFrame ou models
        pass

    def _load_to_database(self, overwrite=False):
        # insere na base via SQLAlchemy
        pass
