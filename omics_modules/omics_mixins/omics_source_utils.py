# #############################################################################
# UTILITY

# #############################################################################
import zlib


class SourceUtilMixin:

    def zfile(self, fileName, splitChar="\n", chunkSize=1 * 1024 * 1024):
        # autodetect gzip or zlib header
        dc = zlib.decompressobj(zlib.MAX_WBITS | 32)
        with open(fileName, "rb") as filePtr:
            text = ""
            while dc:
                data = filePtr.read(chunkSize)
                if data:
                    decompressedData = dc.decompress(data)
                    text += decompressedData.decode("utf-8")
                    data = None
                else:
                    text += dc.flush().decode("utf-8")
                    dc = None
                if text:
                    lines = text.split(splitChar)
                    i, x = 0, len(lines) - 1
                    text = lines[x]
                    while i < x:
                        yield lines[i]
                        i += 1
                    lines = None
            # while data remains
            if text:
                yield text
        # with fileName
