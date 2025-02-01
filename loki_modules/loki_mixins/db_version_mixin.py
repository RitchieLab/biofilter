import apsw


class DbVersionMixin:
    """
    Mixin to provide version and driver information for the LOKI.
    """

    @classmethod
    def getVersionTuple(cls):
        """
        Returns the version information of the database as a tuple.

        Returns:
            tuple: A tuple containing (major, minor, revision, dev, build, dt).
        """
        return (3, 0, 1, "dev", "", "2025-01-01")

    @classmethod
    def getVersionString(cls):
        """
        Returns the version information of the database as a formatted string.

        Returns:
            str: A formatted version string.
        """
        v = list(cls.getVersionTuple())
        v[3] = "" if v[3] > "rc" else v[3]
        return "%d.%d.%d%s%s (%s)" % tuple(v)

    @classmethod
    def getDatabaseDriverName(cls):
        """
        Returns the name of the database driver.

        Returns:
            str: The database driver name.
        """
        return "SQLite"

    @classmethod
    def getDatabaseDriverVersion(cls):
        """
        Returns the version of the SQLite library.

        Returns:
            str: The SQLite library version.
        """
        return apsw.sqlitelibversion()

    @classmethod
    def getDatabaseInterfaceName(cls):
        """
        Returns the name of the database interface.

        Returns:
            str: The database interface name.
        """
        return "APSW"

    @classmethod
    def getDatabaseInterfaceVersion(cls):
        """
        Returns the version of the APSW library.

        Returns:
            str: The APSW library version.
        """
        return apsw.apswversion()

    # Add these methods in own class
    def _checkTesting(self):
        """
        Checks and updates the testing setting in the database.

        Returns:
            bool: True if testing settings match, otherwise False.
        """
        now_test = self.getDatabaseSetting("testing")
        if now_test is None or bool(int(now_test)) == bool(self._is_test):
            self.setDatabaseSetting("testing", bool(self._is_test))
            return True
        else:
            return False

    # setTesting(is_test)
