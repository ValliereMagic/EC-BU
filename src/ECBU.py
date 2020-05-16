import pickle
import os.path
import time
import math
import hashlib
from ECBUMediaUpload import ECBUMediaUpload
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


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


def check_if_chunk_exists_or_changed(service, file_chunk: ECBUMediaUpload, folder_id: str, file_chunk_name: str) -> ChangedFile:
    """
    Using the passed google drive service object try to find the chunk with the name file_chunk_name within the folder with id
    folder_id.
    If a file with the name passed exists in the backup folder, hash the local version and compare them.
    If the hashes match, return a ChangedFile showing no changes necessary.
    If the hashes don't match, return a ChangedFile reflecting that.
    If the file doesn't exist at all within the backup folder, return a True change, and a None id
    """
    page_token = None
    while True:
        response = service.files().list(q="mimeType = 'application/octet-stream' and '" + folder_id + "' in parents",
                                        spaces='drive', fields='nextPageToken, files(id, name, md5Checksum)', pageToken=page_token).execute()
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


def backup_chunked_file_piece(service, file_chunk: ECBUMediaUpload, folder_id: str, file_chunk_name: str):
    """
    Using the check_if_chunk_exists function, check whether this file_chunk has already been backed up before
    If it has, but the hashes don't match becuase the local copy has been modified, update the file in google
    drive.
    If it has never been uploaded before, create the new file in google drive with a name of file_chunk_name.

    Upload the file in a resumable manner, in case the network goes down during a backup for a few minutes
    or hours :/, then the upload can resume; requiring not have to re-upload the pieces of the chunk; this
    hopefully can save precious bandwidth.
    """
    print("Beginning upload of chunk: " + file_chunk_name + ".")
    # Check whether this chunk has been uploaded before
    file_status: ChangedFile = check_if_chunk_exists_or_changed(
        service, file_chunk, folder_id, file_chunk_name)
    # Upload the file_chunk to google drive
    request = None
    # Chunk has never been uploaded before
    if file_status.changed and not file_status.ident:
        request = service.files().create(
            media_body=file_chunk, body={'name': file_chunk_name, 'parents': [folder_id]})
    # Chunk has been uploaded before but it has been changed
    elif file_status.changed and file_status.ident:
        request = service.files().update(
            media_body=file_chunk, fileId=file_status.ident)
    # The chunk has not changed and does not need to be re-uploaded.
    else:
        print("Chunk: " + file_chunk_name + " is already up to date!")
        return
    # beginning back-off duration for if an error occurs and we try to resume
    default_backoff_time: float = 0.5
    backoff_time: float = default_backoff_time
    response = None
    while response is None:
        # Attempt to upload a chunk of the file
        try:
            status, response = request.next_chunk()
        # Catch any errors that occur, attempting to continue
        # uploading if possible
        except HttpError as e:
            if e.resp.status in [404]:
                # Restart upload
                print("Error 404. Must restart upload.")
                return False
            elif e.resp.status in [500, 502, 503, 504]:
                # Call next chunk again, using exponential backoff
                print("An error occurred. Trying again with exponential backoff. Waiting: " +
                      str(backoff_time) + " seconds.")
                time.sleep(backoff_time)
                # Stop waiting for longer and longer times after 2 minutes.
                if backoff_time < 120:
                    backoff_time *= 2
                response = None
                continue
            else:
                # Error and quit
                print("Fatal Error uploading chunk.")
                return False
        # Reset exponential backoff time amount
        backoff_time = default_backoff_time
        if status:
            print("Chunk upload progress: %d%%." %
                  int(status.progress() * 100))
    print("Upload of Chunk: " + file_chunk_name + " Complete!")


def find_or_create_backup_folder(service, dest_folder_name: str) -> str:
    """
    Using the passed drive service object, either find the folder with dest_folder_name
    in the root of google drive, or if it isn't there, create it.
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
    if folder_id is None:
        result = service.files().create(body={
            'name': dest_folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }, fields='id').execute()
        folder_id = result.get('id')
    return folder_id


def begin_backup(service, local_file_name: str, dest_folder_name: str, file_chunk_size: int) -> bool:
    """
    service: google drive service
    local_file_name: name of the file on disk
    dest_folder_name: the name of the folder for this to be stored in on google drive
    file_chunk_size: the size in MB for each of the chunks in the uploaded folder to be.
    """
    # Get or create the parent folder for our chunked backup file
    folder_id: str = find_or_create_backup_folder(service, dest_folder_name)
    # Unable to find or make a folder to back up the file to
    if folder_id is None:
        return False
    # Open up the file and start chunking
    with open(local_file_name, 'rb') as test_file:
        # Calculate the size of the file to backup
        test_file.seek(os.SEEK_SET)
        test_file.seek(0, os.SEEK_END)
        file_size: int = test_file.tell()
        # Calculate the number of file_chunk_size chunks to separate and upload
        file_chunk_size_MB = (file_chunk_size * (1000 * 1000))
        num_chunk_files: int = math.ceil(file_size / file_chunk_size_MB)
        # Create a Media upload for each of the separated files and upload each of them
        bytes_uploaded: int = 0
        for chunk_num in range(1, num_chunk_files + 1):
            # Find the end index for the current file chunk
            end_index: int = bytes_uploaded + file_chunk_size_MB
            # If this is the last chunk, and it goes out of bounds,
            # shorten it so that it doesn't
            if end_index >= file_size:
                end_index = file_size - 1
            # Create the ECBUMediaUpload object to represent this chunk of the file
            file_chunk = ECBUMediaUpload(
                test_file, file_size, bytes_uploaded, end_index)
            # Upload this chunk to google drive
            backup_chunked_file_piece(
                service, file_chunk, folder_id, dest_folder_name + '.' + str(chunk_num))
            bytes_uploaded += file_chunk_size_MB
        return True


def get_credentials():
    """
    Load the required credentials to access the google drive API
    """
    credentials = None
    # Check if we have saved credentials
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    # Request credentials
    if not credentials or not credentials.valid:
        # Credentials require refreshing
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        # Need to request credentials
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            credentials = flow.run_local_server(port=0)
            # Save the credentials to the token file
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)
    return credentials


def main():
    # Acquire required credentials for google drive
    credentials = get_credentials()
    if credentials is None:
        print("Unable to acquire credentials")
        return
    # Create the drive service
    service = build('drive', 'v3', credentials=credentials)
    # Begin backing up the file.
    begin_backup(
        service, 'test_file', 'TestFile', 5)


if __name__ == '__main__':
    main()
