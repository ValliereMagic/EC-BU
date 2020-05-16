import hashlib
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


class ChangedFile:
    """
    Simple struct returned by check_if_chunk_exists to detect chunk changes
    (Which need to be uploaded and which don't)
    """

    def __init__(self, changed: bool, ident: str):
        # Boolean value of whether the file has changed
        self.changed = changed
        # string value of the file id in google drive
        self.ident = ident


def check_if_chunk_exists_or_changed(service, file_chunk: ECBUMediaUpload,
                                     folder_id: str, file_chunk_name: str) -> ChangedFile:
    """
    Using the passed google drive service object try to find the chunk with the name file_chunk_name
    within the folder with id folder_id.
    If a file with the name passed exists in the backup folder, hash the local version and compare them.
    If the hashes match, return a ChangedFile showing no changes necessary.
    If the hashes don't match, return a ChangedFile reflecting that.
    If the file doesn't exist at all within the backup folder, return a True change, and a None id
    """
    page_token = None
    while True:
        response = service.files().list(q="mimeType = 'application/octet-stream' and '" + folder_id + "' in parents",
                                        spaces='drive', fields='nextPageToken, files(id, name, md5Checksum)',
                                        pageToken=page_token).execute()
        for file in response.get('files', []):
            file_name: str = file.get('name')
            # Check if this is a match
            if file_chunk_name == file_name:
                file_id: str = file.get('id')
                md5hash: str = file.get('md5Checksum')
                local_hash: str = hash_ecbu_media_file_upload(file_chunk)
                # Check whether this chunk has changed since last time
                # it was uploaded by comparing the hashes.
                if md5hash == local_hash:
                    return ChangedFile(False, None)
                else:
                    return ChangedFile(True, file_id)
        # Move on to the next page
        page_token = response.get('nextPageToken', None)
        # No more pages to look through
        if page_token is None:
            break
    # The file was not found, hash it and return
    return ChangedFile(True, None)
