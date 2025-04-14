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
