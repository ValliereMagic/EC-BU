# STL resources
import re
import time

# ECBU modules
from ChunkChanges import hash_ecbu_media_file_upload
from UploadAbstraction import ECBUMediaUpload


class ChangedFile:
    """
    Simple struct returned by check_if_chunk_exists to detect chunk changes
    (Which need to be uploaded and which don't)
    """

    def __init__(self, changed: bool, file_id: str):
        # Boolean value of whether the file has changed
        self.changed = changed
        # string value of the file id in google drive
        self.file_id = file_id


class DriveChunks(object):
    """
    Holds a cached list of all the chunks stored in google drive for the current
    backup or restore.
    """

    def __init__(self, service: object, folder_id: str):
        self._service = service
        self.folder_id = folder_id
        self._chunk_changes_cache = None

    @staticmethod
    def _chunk_name_to_num(chunk_name: str):
        """
        Remove everything from the chunk_name up to and
        including the '.', then convert it to an integer.
        i.e. isolate the 23 in HHS.23
        """
        return int(re.sub(r'^(.*)\.', '', chunk_name))

    @staticmethod
    def _chunk_id_response_compare(item: dict):
        """
        Key function for sorting responses of
        files within a backup folder.
        """
        return DriveChunks._chunk_name_to_num(item.get('name'))

    def _refresh_cache(self):
        """
        Refresh local list of chunks stored up in google drive
        """
        self._chunk_changes_cache = list()
        page_token = None
        while True:
            response = self._service.files().list(q="'{}' in parents and trashed = false".format(self.folder_id),
                                                  spaces='drive',
                                                  fields='nextPageToken, files(id, name, size, md5Checksum)',
                                                  pageToken=page_token).execute()
            # Append the chunk information.
            self._chunk_changes_cache += response.get('files', [])
            # Move on to the next page
            page_token = response.get('nextPageToken', None)
            # No more pages to look through
            if page_token is None:
                break
            # Sleep for 90ms to make sure our request does not exceed the google drive
            # limit of 1000 requests per 100 seconds per user
            time.sleep(0.09)
        # Sort the chunk_information so that we restore the folder in the correct order
        self._chunk_changes_cache.sort(key=self._chunk_id_response_compare)

    def get_chunk_file_information(self, refresh_cache: bool = False) -> list:
        """
        Query google drive if cache isn't adequate and acquire the ids in order of each
        chunk making up the file to restore.
        """
        if not self._chunk_changes_cache or refresh_cache:
            # Query the results from google drive
            self._refresh_cache()
        return self._chunk_changes_cache

    def check_if_chunk_exists_or_changed(self, file_chunk: ECBUMediaUpload,
                                         file_chunk_name: str,
                                         refresh_cache: bool = False) -> ChangedFile:
        """
        It will only query google drive the first time it runs, getting all the ids and
        checksums once and referencing the cache for subsequent queries. Due to this, if
        a chunk changes for some reason during the upload process (like more than one ECBU
        was running or something), those changes would not be reflected in querying this
        function. Unless refesh_cache is set to True.

        If a file with the name passed exists in the backup folder, hash the local version
        and compare them. If the hashes match, return a ChangedFile showing no changes necessary.
        If the hashes don't match, return a ChangedFile reflecting that.
        If the file doesn't exist at all within the backup folder, return a True change, and a
        None id
        """
        if not self._chunk_changes_cache or refresh_cache:
            # Query the results from google drive
            self._refresh_cache()
        # Get the index for where we should find the chunk in the cache
        chunk_idx: int = (self._chunk_name_to_num(file_chunk_name)) - 1
        # Make sure the chunk_idx isn't out of bounds
        if chunk_idx > (len(self._chunk_changes_cache) - 1):
            # The file was not found
            return ChangedFile(True, None)
        # Get the chunk
        chunk: object = self._chunk_changes_cache[chunk_idx]
        # Verify the name is correct
        if file_chunk_name == chunk.get('name'):
            file_id: str = chunk.get('id')
            md5hash: str = chunk.get('md5Checksum')
            local_hash: str = hash_ecbu_media_file_upload(
                file_chunk)
            # Check whether this chunk has changed since last time
            # it was uploaded by comparing the hashes.
            if md5hash == local_hash:
                return ChangedFile(False, None)
            # Otherwise the chunk has been changed, and needs to be
            # re-uploaded.
            return ChangedFile(True, file_id)
        # The file was not found
        return ChangedFile(True, None)


def find_or_create_backup_folder(service, dest_folder_name: str,
                                 create_mode: bool = True) -> str:
    """
    Using the passed drive service object, either find the folder with dest_folder_name
    in the root of google drive, or if it isn't there, create it when create mode is true.
    """
    folder_id = None
    page_token = None
    while True:
        response = service.files().list(q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
                                        spaces='drive', fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        # Look through each of the files accessable to this application, and see if there is already a folder
        # there for backing up this file.
        for file in response.get('files', []):
            folder_name: str = file.get('name')
            file_id: str = file.get('id')
            # Found a folder to upload this file to
            # already.
            if folder_name == dest_folder_name:
                folder_id = file_id
        # Move on to the next page
        page_token = response.get('nextPageToken', None)
        # No more pages to look through
        if page_token is None:
            break
    # Folder doesn't exist, we need to create one
    if folder_id is None and create_mode:
        result = service.files().create(body={
            'name': dest_folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }, fields='id').execute()
        folder_id = result.get('id')
    return folder_id
