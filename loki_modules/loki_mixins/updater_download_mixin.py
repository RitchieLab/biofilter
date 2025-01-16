# #################################################
# UPDATE DOWNLOAD MIXIN
# #################################################
import hashlib
import os


class UpdaterDownloadMixin:

    def downloadAndHash(self, iwd, srcName, srcOptions):
        srcObj = self._sourceObjects[srcName]
        # srcID = srcObj.getSourceID()
        options = self._sourceOptions[srcName]

        try:
            self.log("downloading %s data ...\n" % srcName)
            # switch to a temp subdirectory for this source
            path = os.path.join(iwd, srcName)
            if not os.path.exists(path):
                os.makedirs(path)
            downloadedFiles = srcObj.download(options, path)
            self.log("downloading %s data completed\n" % srcName)

            # calculate source file metadata
            # all timestamps are assumed to be in UTC, but if a source
            # provides file timestamps with no TZ (like via FTP) we use them
            # as-is and assume they're supposed to be UTC
            self.log("analyzing %s data files ...\n" % srcName)
            for filename in downloadedFiles:
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
            self.log("analyzing %s data files completed\n" % srcName)
        except:  # noqa: E722
            self.log("failed loading %s\n" % srcName)
            # ToDo: determine how to handle failures