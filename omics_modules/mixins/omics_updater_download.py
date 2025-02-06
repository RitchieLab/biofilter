# #################################################
# UPDATE DOWNLOAD MIXIN 
# #################################################
import hashlib
import os


class UpdaterDownloadMixin:

    def file_hash(self, filename):
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

    def workflow_download(self, iwd, srcName, srcOptions):

        srcObj = self._sourceObjects[srcName]
        options = self._sourceOptions[srcName]

        try:

            # switch to a temp subdirectory for this source
            path = os.path.join(iwd, srcName)
            if not os.path.exists(path):
                os.makedirs(path)
            downloadedFiles = srcObj.download(options, path) # TODO: podemos colocar os retornos na tabela

            # calculate source file metadata
            # all timestamps are assumed to be in UTC, but if a source
            # provides file timestamps with no TZ (like via FTP) we use them
            # as-is and assume they're supposed to be UTC

            
            # for filename in downloadedFiles:
            #     self.fileHash(filename)

            return True, None  # ✅ Download successful


        except Exception as e:
            # No raise exception to continue other sources

            # Remove the source from the list of sources to avoid run process
            if srcName in self.srcSetsToDownload:
                self.srcSetsToDownload.remove(srcName)

            msn_error = f"Error downloading {srcName} data: {str(e)}"
            # self._loki.addWarning(srcObj._sourceID, msn_error)

            return False, msn_error  # ❌ Download failed

