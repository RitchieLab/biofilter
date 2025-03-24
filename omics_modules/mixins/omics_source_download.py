# #############################################################################
# CONNECTORS
# This mixin provides methods to connect source systems and retrieve data from
# them.
# #############################################################################
import ftplib
import os
import time
import urllib
import urllib.request as urllib2
import wget
from datetime import datetime, timezone
import logging
import gc


class SourceConnectorMixin:

    def downloadFilesFromFTP(self, remHost, remFiles):
        # remFiles=function(ftp) or
        # {'filename.ext':'/path/on/remote/host/to/filename.ext',...}
        # connect to source server
        # self.log(
        #     "connecting to FTP server %s ..." % remHost,
        #     level=logging.INFO,
        # )  # noqa E501
        ftp = ftplib.FTP(remHost, timeout=21600)
        ftp.login()  # anonymous
        self.log(" OK\n", level=logging.INFO)

        # if remFiles is callable, let it identify the files it wants
        if hasattr(remFiles, "__call__"):
            self.log("locating current files ...", level=logging.INFO)
            remFiles = remFiles(ftp)
            self.log(" OK\n", level=logging.INFO)

        # check local file sizes and times, and identify
        # all needed remote paths
        remDirs = set()
        remSize = {}
        remTime = {}
        locSize = {}
        locTime = {}
        for locPath, remFile in remFiles.items():
            remDirs.add(remFile[0 : remFile.rfind("/")])  # noqa E203

            remSize[remFile] = None
            remTime[remFile] = None
            locSize[locPath] = None
            locTime[locPath] = None
            if os.path.exists(locPath):
                stat = os.stat(locPath)
                locSize[locPath] = int(stat.st_size)
                locTime[locPath] = datetime.fromtimestamp(stat.st_mtime)  # noqa E501

        now = datetime.now(timezone.utc)

        def ftpDirCB(rem_dir, line):
            words = line.split()
            remFn = rem_dir + "/" + words[8]
            if len(words) >= 9 and remFn in remSize:
                remSize[remFn] = int(words[4])
                timestamp = " ".join(words[5:8])
                try:
                    time = datetime.strptime(timestamp, "%b %d %Y")
                except ValueError:
                    try:
                        time = datetime.strptime(
                            "%s %d" % (timestamp, now.year), "%b %d %H:%M %Y"
                        )
                    except ValueError:
                        try:
                            time = datetime.strptime(
                                "%s %d" % (timestamp, now.year - 1),
                                "%b %d %H:%M %Y",  # noqa E501
                            )
                        except ValueError:
                            time = now
                    if (time.year == now.year and time.month > now.month) or (
                        time.year == now.year
                        and time.month == now.month
                        and time.day > now.day
                    ):
                        time = time.replace(year=now.year - 1)
                remTime[remFn] = time

        # check remote file sizes and times
        self.log("identifying changed files ...")
        for remDir in remDirs:
            ftp.dir(remDir, lambda x: ftpDirCB(remDir, x))
        self.log(" OK\n")

        # download files as needed
        self.log("downloading changed files ...\n")
        for locPath in sorted(remFiles.keys()):
            if (
                remSize[remFiles[locPath]] == locSize[locPath]
                and remTime[remFiles[locPath]] <= locTime[locPath]
            ):
                # self.log(
                #     "%s: up to date\n" % locPath, level=logging.INFO, indent=1
                # )  # noqa E501
                pass
            else:
                # self.log(
                #     "%s: downloading ...\n" % locPath, level=logging.INFO
                # )  # noqa E501
                # TODO: download to temp file, then rename?
                with open(locPath, "wb") as locFile:
                    # ftp.cwd(remFiles[locPath][0:remFiles[locPath].rfind('/')])
                    ftp.retrbinary("RETR " + remFiles[locPath], locFile.write)

                # TODO: verify file size and retry a few times if necessary

                # self.log("... OK\n", level=logging.INFO, indent=1)

            modTime = time.mktime(remTime[remFiles[locPath]].utctimetuple())
            os.utime(locPath, (modTime, modTime))

        # disconnect from source server
        try:
            ftp.quit()
        except Exception:
            ftp.close()

        # self.log(
        #     "... OK\n",
        #     level=logging.INFO,
        #     indent=0,
        # )

    def getHTTPHeaders(self, remHost, remURL, reqData=None, reqHeaders=None):
        class NoRedirection(urllib2.HTTPErrorProcessor):
            def http_response(self, request, response):
                return response

            https_response = http_response

        # NoRedirection
        opener = urllib2.build_opener(NoRedirection)

        if reqData and not isinstance(reqData, str):
            reqData = urllib.parse.urlencode(reqData, doseq=True)
        request = urllib2.Request(
            url="http://" + remHost + remURL,
            data=reqData,
            headers=(reqHeaders or {}),  # noqa E501
        )
        if not reqData:
            request.get_method = lambda: "HEAD"
        response = opener.open(request)
        respInfo = response.info()
        respHeaders = dict((h.lower(), respInfo[h]) for h in respInfo)
        response.close()
        return respHeaders

    def downloadFilesFromHTTP(
        self, remHost, remFiles, reqHeaders=None, alwaysDownload=False
    ):
        return self._downloadHTTP(
            "http", remHost, remFiles, reqHeaders, alwaysDownload
        )  # noqa E501

    def downloadFilesFromHTTPS(
        self, remHost, remFiles, reqHeaders=None, alwaysDownload=False
    ):
        return self._downloadHTTP(
            "https", remHost, remFiles, reqHeaders, alwaysDownload
        )  # noqa E501

    def _downloadHTTP(
        self, remProtocol, remHost, remFiles, reqHeaders, alwaysDownload
    ):  # noqa E501
        _indent = 4
        remSize = {}
        locSize = {}
        max_retries = 3  # max number of retries per file
        retry_delay = 5  # delay between retries in seconds

        # Check local file sizes
        for locPath in remFiles:
            remSize[locPath] = None
            locSize[locPath] = None
            if os.path.exists(locPath):
                stat = os.stat(locPath)
                locSize[locPath] = int(stat.st_size)

        # Download files as needed
        for locPath in sorted(remFiles.keys()):
            retries = 0
            success = False

            while retries < max_retries and not success:
                try:
                    link = remProtocol + "://" + remHost + remFiles[locPath]

                    self.logger.log(
                        # f"{locPath}: downloading (attempt {retries + 1}/{max_retries}) ...",  # noqa E501
                        f"Downloading (attempt {retries + 1}/{max_retries}) from {link}",  # noqa E501
                        # level=logging.INFO,
                        # indent=_indent
                    )

                    if remProtocol == "https":
                        with open(locPath, "wb") as locFile:
                            request = urllib2.Request(link)
                            request.add_header("user-agent", "RitchieLab/LOKI")
                            for k, v in (reqHeaders or {}).items():
                                request.add_header(k, v)

                            # Perform the download
                            response = urllib2.urlopen(request)
                            chunk_size = 1024 * 1024  # 1 MB
                            while True:
                                data = response.read(chunk_size)
                                if not data:
                                    break
                                locFile.write(data)
                                del data  # free memory
                                gc.collect()  # collect garbage
                            response.close()
                            # Progress bar showed only
                            print("  ✅ Download complete.")

                    else:
                        # For non-HTTPS protocols, use wget as fallback
                        wget.download(link)
                        os.rename(remFiles[locPath].rsplit("/")[-1], locPath)
                        print("  ✅ Download complete.")

                    success = True

                except Exception as e:
                    retries += 1
                    # print('❌ Error occurred during download.')
                    # self.log(
                    #     f"Error downloading (attempt {retries}/{max_retries}): {str(e)}",  # noqa E501
                    #     level=logging.ERROR,
                    #     indent=_indent,
                    # )
                    if retries < max_retries:
                        # self.log(
                        #     f"Retrying download in {retry_delay} seconds.",
                        #     level=logging.WARNING,
                        #     indent=_indent,
                        # )
                        time.sleep(retry_delay)
                    # else:
                    #     self.log(
                    #         f"Max retries reached for {locPath}. Skipping this file.",  # noqa E501
                    #         level=logging.WARNING,
                    #         indent=_indent,
                    #     )

            # If not successful after all attempts, log the final error
            if not success:
                # self.log(
                #     f"Failed to download {locPath} after {max_retries} attempts.",  # noqa E501
                #     level=logging.ERROR,
                #     indent=_indent,
                # )
                # Opcional: raise para interromper o processo completamente
                # raise RuntimeError(f"Failed to download {locPath}")
                continue

        # self.log(
        #     "All downloads completed.",
        #     level=logging.INFO,
        #     indent=_indent
        # )
