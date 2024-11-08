# #################################################
# LOGING MIXIN
# #################################################
import sys


class LoggerMixin:
    """
    Logger is a centralized logging utility that provides structured logging
    with indentation management. This class allows consistent logging across
    different components and can differentiate between standard log messages
    and warnings. It supports logging to both standard output and an optional
    log file, with control over indentation levels for better readability.

    IMPLEMENTED METHODS:
    - [log]: Logs a standard message to both file and standard output if apply.
    - [warn]: Logs a warning message.
    - [logPush]: Increases the indentation level and optionally logs a message.
    - [logPop]: Decreases the indentation level and optionally logs a message.
    - [warnPush]: Increases indentation for warnings.
    - [warnPop]: Decreases indentation for warnings.
    """

    def _log(self, message="", warning=False):
        if (self._logIndent > 0) and (not self._logHanging):
            if self._logFile:
                self._logFile.write(self._logIndent * "  ")
            if self._verbose or (warning and not self._quiet):
                sys.stderr.write(self._logIndent * "  ")
            self._logHanging = True

        if self._logFile:
            self._logFile.write(message)
        if self._verbose or (warning and not self._quiet):
            sys.stderr.write(message)

        if message[-1:] != "\n":
            if self._logFile:
                self._logFile.flush()
            if self._verbose or (warning and not self._quiet):
                sys.stderr.flush()
            self._logHanging = True
        else:
            self._logHanging = False

    def log(self, message=""):
        self._log(message, False)

    def logPush(self, message=None):
        if message:
            self.log(message)
        if self._logHanging:
            self.log("\n")
        self._logIndent += 1

    def logPop(self, message=None):
        if self._logHanging:
            self.log("\n")
        self._logIndent = max(0, self._logIndent - 1)
        if message:
            self.log(message)

    def warn(self, message=""):
        self._log(message, True)

    def warnPush(self, message=None):
        if message:
            self.warn(message)
        if self._logHanging:
            self.warn("\n")
        self._logIndent += 1

    def warnPop(self, message=None):
        if self._logHanging:
            self.warn("\n")
        self._logIndent = max(0, self._logIndent - 1)
        if message:
            self.warn(message)
