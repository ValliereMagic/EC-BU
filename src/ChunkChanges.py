# STL resources
import hashlib

# ECBU modules
from UploadAbstraction import ECBUMediaUpload


def hash_ecbu_media_file_upload(file_chunk: ECBUMediaUpload) -> str:
    """
    md5 hash the contents of the passed file_chunk, and return
    it as a hexstring
    """
    hasher = hashlib.md5()
    # Append each chunk of the file to the hasher
    bytes_hashed: int = 0
    while bytes_hashed < file_chunk.size():
        current_chunk: bytes = file_chunk.getbytes(
            bytes_hashed, file_chunk.chunksize())
        bytes_hashed += len(current_chunk)
        hasher.update(current_chunk)
    # Turn it into a hex_digest and return
    return hasher.hexdigest()
