# #################################################
# LOGING MIXIN
# #################################################
import datetime


class LoggerMixin:
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

    def getVerbose(self):
        """
        Gets the verbosity setting.

        Returns:
            bool: True if verbose logging is enabled, otherwise False.
        """
        return self._verbose

    # getVerbose()

    def setVerbose(self, verbose=True):
        """
        Sets the verbosity setting.

        Args:
            verbose (bool, optional): True to enable verbose logging, False to
            disable.
        """
        self._verbose = verbose

    # setVerbose()

    def setLogger(self, logger=None):
        """
        Sets the logger object.

        Args:
                logger (Logger, optional): The logger object.
        """
        self._logger = logger

    def log(self, message=""):
        """
        Logs a message to the configured logger or standard output with
        indentation.

        Args:
            message (str, optional): The message to log. Defaults to an empty
            string.

        Returns:
            int: The current indentation level.

        The function logs the message with appropriate indentation and handles
        line breaks. If a logger is set, it uses the logger to log the message.
        If verbose logging is enabled, it writes the message to the standard
        output with indentation.
        """
        if message != "" and message != "\n":
            logtime = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
            message = logtime + " " + message

        if self._logger:
            return self._logger.log(message)
        if self._verbose:
            if (self._logIndent > 0) and (not self._logHanging):
                self._logFile.write(self._logIndent * "  ")
                self._logHanging = True
            self._logFile.write(message)
            if (message == "") or (message[-1] != "\n"):
                self._logHanging = True
                self._logFile.flush()
            else:
                self._logHanging = False
        return self._logIndent

    # log()

    def logPush(self, message=None):
        """
        Logs a message and increases the indentation level.

        Args:
            message (str, optional): The message to log. Defaults to None.

        Returns:
            int: The new indentation level.

        The function logs the message if provided and increases the
        indentation level for subsequent logs. If a logger is set, it uses the
        logger to log the message.
        """

        if self._logger:
            return self._logger.logPush(message)
        if message:
            self.log(message)
        if self._logHanging:
            self.log("\n")
        self._logIndent += 1
        return self._logIndent

    # logPush()

    def logPop(self, message=None):
        """
        Decreases the indentation level and logs a message.

        Args:
            message (str, optional): The message to log. Defaults to None.

        Returns:
            int: The new indentation level.

        The function decreases the indentation level and logs the message if
        provided. If a logger is set, it uses the logger to log the message.
        """

        if self._logger:
            return self._logger.logPop(message)
        if self._logHanging:
            self.log("\n")
        self._logIndent = max(0, self._logIndent - 1)
        if message:
            self.log(message)
        return self._logIndent
