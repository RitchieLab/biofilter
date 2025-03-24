# #################################################
# UPDATE DOWNLOAD MIXIN
# #################################################
import hashlib
import os
import logging


class UpdaterDownloadMixin:

    def fileHash(self, filename):
        """
        Calculate file metadata (size, mtime, MD5 hash) for a given file.

        Args:
            filename (str): Path to the file.

        Returns:
            tuple: A tuple containing (filename, size, mtime, md5_hexdigest).
        """
        stat = os.stat(filename)
        md5 = hashlib.md5()
        with open(filename, "rb") as f:
            chunk = f.read(8 * 1024 * 1024)
            while chunk:
                md5.update(chunk)
                chunk = f.read(8 * 1024 * 1024)

            self.lock.acquire()
            self._filehash[filename] = (
                filename,
                int(stat.st_size),
                int(stat.st_mtime),
                md5.hexdigest(),
            )
            self.lock.release()

    def downloadAndHash(self, iwd, srcName, srcOptions):
        _intend = 2

        srcObj = self._sourceObjects[srcName]
        options = self._sourceOptions[srcName]

        try:
            self.log(
                "Thread - Downloading %s data ..." % srcName,
                level=logging.INFO,
                indent=_intend,
            )
            # switch to a temp subdirectory for this source
            path = os.path.join(iwd, srcName)
            if not os.path.exists(path):
                os.makedirs(path)
            downloadedFiles = srcObj.download(options, path)
            self.log(
                "Thread - Downloading %s data completed" % srcName,
                level=logging.INFO,
                indent=_intend,
            )

            # calculate source file metadata
            # all timestamps are assumed to be in UTC, but if a source
            # provides file timestamps with no TZ (like via FTP) we use them
            # as-is and assume they're supposed to be UTC
            self.log(
                "Thread - Analyzing %s data files ..." % srcName,
                level=logging.INFO,
                indent=_intend,
            )
            for filename in downloadedFiles:
                self.fileHash(filename)

            self.log(
                "Thread - Analyzing %s data files completed" % srcName,
                level=logging.INFO,
                indent=_intend,
            )
        except Exception as e:
            # No raise exception to continue other sources
            # Log the exception
            self.log_exception(e)

            # Remove the source from the list of sources to avoid run process
            if srcName in self.srcSetsToDownload:
                self.srcSetsToDownload.remove(srcName)

            msn_error = f"Error downloading {srcName} data: {str(e)}"
            self._loki.addWarning(srcObj._sourceID, msn_error)

            # TODO Add filed in Source to control the status
            # TODO Add arg to inform folder with files to avoid download again
