import os
import importlib
import omics_modules.omics_source as omics_source
import omics_modules.sources as source_systems


class UpdaterSourceMixin:
    """
    Mixin responsible for discovering, loading, and instantiating source modules.
    """

    def findSourceModules(self):
        """
        Discovers available source modules in the `omics_modules/sources` directory
        and updates the `_sourceSystems` dictionary.
        """
        if not self._sourceSystems:
            self._sourceSystems = {}
            source_system_path = source_systems.__path__
            for path in source_system_path:
                for srcModuleName in os.listdir(path):
                    if srcModuleName.startswith("omics_source_"):
                        module_name = srcModuleName[13:-3]  # Extract module name
                        self._sourceSystems[module_name] = 1

    def loadSourceModules(self, sources=None):
        """
        Loads the source modules dynamically based on the available source files.

        Args:
            sources (list, optional): Specific sources to load. Defaults to None.

        Returns:
            set: A set of successfully loaded source names.
        """
        if self._sourceSystems is None:
            self.findSourceModules()

        srcSet = set()

        # If no sources are specified, load all available sources
        for srcName in sources if sources else self._sourceSystems.keys():
            if srcName not in self._sourceClasses:
                if srcName not in self._sourceSystems:
                    self.logger.log(f"[WARNING] Unknown source: {srcName}")
                    continue

                try:
                    srcModule = importlib.import_module(
                        f"{source_systems.__name__}.omics_source_{srcName}"
                    )
                    srcClass = getattr(srcModule, f"Source_{srcName}")

                    if not issubclass(srcClass, omics_source.Source):
                        self.logger.log(f"[WARNING] Invalid module for source: {srcName}")
                        continue

                    self._sourceClasses[srcName] = srcClass
                    self.logger.log(f"[INFO] Loaded source module: {srcName}")

                except Exception as e:
                    self.logger.log(f"[ERROR] Failed to load source module {srcName}: {e}")
                    continue

            srcSet.add(srcName)

        return srcSet

    def attachSourceModules(self, sources=None):
        """
        Instantiates source classes and retrieves their corresponding database records.

        Args:
            sources (list, optional): List of sources to attach. Defaults to None.

        Returns:
            list: List of `DataSource` objects from the database.
        """
        sources_to_load = self.loadSourceModules(sources)

        for srcName in sources_to_load:
            if srcName not in self._sourceObjects:
                if srcName not in self._sourceClasses:
                    raise Exception(f"[ERROR] loadSourceModules() reported false positive for '{srcName}'")

                # Instantiate source class
                self._sourceObjects[srcName] = self._sourceClasses[srcName](self._database)
                self._sourceOptions[srcName] = self._sourceObjects[srcName].getOptions()
                self._sourceVersions[srcName] = self._sourceObjects[srcName].getVersionString()
