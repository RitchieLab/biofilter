import hashlib


def compute_file_hash(file_path, chunk_size=8192):
    """
    Computes a SHA256 hash of a file without loading it entirely into memory.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_files_hash(file_paths, chunk_size=8192):
    """
    Computes a single SHA256 hash over an ordered list of files.

    Used when a data source is made of more than one downloaded file and
    the ETL skip logic needs one hash covering all of them.
    """
    sha256 = hashlib.sha256()
    for file_path in file_paths:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                sha256.update(chunk)
    return sha256.hexdigest()
