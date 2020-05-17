# STL resources
import os.path
import time
import math
from argparse import ArgumentParser, Namespace
# ECBU modules
from UploadAbstraction import ECBUMediaUpload
from ChunkChanges import ChangedFile, check_if_chunk_exists_or_changed
from Credentials import get_drive_service
# Google API libraries
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
        return True
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
    return True


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


def begin_backup(service, local_file_name: str, dest_folder_name: str,
                 file_chunk_size: int = 1000, upload_chunk_size: int = 1) -> bool:
    """
    service: google drive service
    local_file_name: name of the file on disk
    dest_folder_name: the name of the folder for this to be stored in on
        google drive
    file_chunk_size: the size in MB for each of the chunks in the uploaded
        folder to be.
    upload_chunk_size: the size in MiB of the resumable upload chunks for
        uploading the file chunk to google drive.
    """
    # Get or create the parent folder for our chunked backup file
    folder_id: str = find_or_create_backup_folder(service, dest_folder_name)
    # Unable to find or make a folder to back up the file to
    if folder_id is None:
        return False
    # Open up the file and start chunking
    with open(local_file_name, 'rb') as local_file:
        # Calculate the size of the file to backup
        local_file.seek(os.SEEK_SET)
        local_file.seek(0, os.SEEK_END)
        file_size: int = local_file.tell()
        # Calculate the number of file_chunk_size chunks to separate and upload
        file_chunk_size *= (1000 * 1000)
        num_chunk_files: int = math.ceil(file_size / file_chunk_size)
        # Create a Media upload for each of the separated files and upload each of them
        bytes_uploaded: int = 0
        for chunk_num in range(1, num_chunk_files + 1):
            # Find the end index for the current file chunk
            end_index: int = bytes_uploaded + file_chunk_size
            # If this is the last chunk, and it goes out of bounds,
            # shorten it so that it doesn't
            if end_index >= file_size:
                end_index = file_size
            # Create the ECBUMediaUpload object to represent this chunk of the file
            file_chunk = ECBUMediaUpload(
                local_file, file_size, bytes_uploaded, end_index,
                chunk_size=(upload_chunk_size * (1024 * 1024)))
            # Upload this chunk to google drive
            status: bool = False
            while status is False:
                # Attempt to upload the chunk
                status = backup_chunked_file_piece(
                    service, file_chunk, folder_id, dest_folder_name + '.' + str(chunk_num))
                # If successful continue, otherwise wait for a second and try again.
                if status:
                    break
                print("Upload of this chunk failed in non-resumable way. Re-Attempting the upload "
                      "in 1 second.")
                time.sleep(1)
            # record the number of bytes uploaded
            # and move the index over one to not re-upload the end index of the
            # previous chunk as the start index of the next.
            bytes_uploaded += file_chunk.size()
        print("Upload of: " + local_file_name + " as " +
              dest_folder_name + " was successful.")
        return True


def parse_integer_argument(int_arg: str, error_str: str) -> int or None:
    """
    Parse a user entered integer argument as an integer from a string.
    If an error occurs, inform the user.
    """
    resultant_int: int = None
    if int_arg:
        try:
            resultant_int = int(int_arg)
            # Make sure the argument isn't out of sensible bounds
            if resultant_int > 1000 or resultant_int < 0:
                print("Chunk arguments should be in 0 < chunk_argument <= 1000")
                return None
        # Passed argument wasn't a string
        except ValueError:
            print(error_str)
            return None
    return resultant_int


def main():
    """
    Grab the CLI arguments passed by the user, and then begin a backup
    """
    # Register the argument parser
    arg_parser: ArgumentParser = ArgumentParser(
        description="EC-BU ~Exposed Conscious Back-Up~")
    # Register the two different argument groups
    required = arg_parser.add_argument_group("required arguments")
    # Register the options the user can pick through
    required.add_argument('--file-to-backup', dest="file_to_backup",
                          help="Local file name to back up in "
                          "chunks to google drive.")
    required.add_argument('--dest-folder-name', dest="dest_folder_name",
                          help="Folder name in google drive to contain the "
                          "chunks of the backed-up file.")
    arg_parser.add_argument('--google-drive-chunk-size', dest="google_drive_chunk_size",
                            help="Size of each file chunk split up in the backup folder. (Megabytes)")
    arg_parser.add_argument('--file-upload-chunk-size', dest="file_upload_chunk_size",
                            help="Chunk size for resumable uploads to the drive service. (MebiBytes)")
    # Parse the arguments entered by the user
    parsed_args: Namespace = arg_parser.parse_args()
    # Make sure all the required arguments are not None
    if parsed_args.file_to_backup is None or \
       parsed_args.dest_folder_name is None:
        arg_parser.print_help()
        return
    # Create the drive service
    service = get_drive_service()
    # Begin backing up the file, with the options picked by the user
    google_drive_chunk_error: str = "Error. Google Drive Chunk size must be an integer."
    file_upload_chunk_error: str = "Error. File Upload Chunk size must be an integer."
    # Both optional arguments were passed
    if parsed_args.google_drive_chunk_size and \
            parsed_args.file_upload_chunk_size:
        # Try and convert the optional arguments to integers
        google_drive_chunk_size: int = parse_integer_argument(
            parsed_args.google_drive_chunk_size, google_drive_chunk_error)
        file_upload_chunk_size: int = parse_integer_argument(
            parsed_args.file_upload_chunk_size, file_upload_chunk_error)
        # None check on integer conversion
        if google_drive_chunk_size is None or \
                file_upload_chunk_size is None:
            return
        begin_backup(
            service, parsed_args.file_to_backup, parsed_args.dest_folder_name,
            google_drive_chunk_size, file_upload_chunk_size)
    # Only the dest_file_chunk size was passed
    elif parsed_args.google_drive_chunk_size:
        # Try and convert the optional argument to an integer
        google_drive_chunk_size: int = parse_integer_argument(
            parsed_args.google_drive_chunk_size, google_drive_chunk_error)
        # None check on integer conversion
        if google_drive_chunk_size is None:
            return
        begin_backup(
            service, parsed_args.file_to_backup, parsed_args.dest_folder_name,
            file_chunk_size=google_drive_chunk_size)
    # Only file upload chunk size was passed
    elif parsed_args.file_upload_chunk_size:
        # Try and convert the optional argument to an integer
        file_upload_chunk_size: int = parse_integer_argument(
            parsed_args.file_upload_chunk_size, file_upload_chunk_error)
        # None check on integer conversion
        if file_upload_chunk_size is None:
            return
        begin_backup(
            service, parsed_args.file_to_backup, parsed_args.dest_folder_name,
            upload_chunk_size=file_upload_chunk_size)
    # No optional arguments were passed
    else:
        begin_backup(
            service, parsed_args.file_to_backup, parsed_args.dest_folder_name)


if __name__ == '__main__':
    main()
