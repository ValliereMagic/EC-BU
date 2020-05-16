import os.path
import time
import math
from UploadAbstraction import ECBUMediaUpload
from ChunkChanges import ChangedFile, check_if_chunk_exists_or_changed
from Credentials import get_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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
