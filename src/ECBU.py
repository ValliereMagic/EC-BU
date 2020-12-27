# STL resources
import math
import os.path
import time
from argparse import ArgumentParser, Namespace

# Google API libraries
from googleapiclient.errors import HttpError

# ECBU modules
from CommandLineParse import parse_integer_argument
from Credentials import get_drive_service
from DriveAccess import ChangedFile, DriveChunks, find_or_create_backup_folder
from ErrorWaiting import IncreasingBackoff
from UploadAbstraction import ECBUMediaUpload


def backup_chunked_file_piece(service: object, drive_chunks: DriveChunks, file_chunk: ECBUMediaUpload,
                              file_chunk_name: str, chunk_num: int, num_chunks: int):
    """
    Using the check_if_chunk_exists function, check whether this file_chunk has already been backed up before
    If it has, but the hashes don't match becuase the local copy has been modified, update the file in google
    drive.
    If it has never been uploaded before, create the new file in google drive with a name of file_chunk_name.

    Upload the file in a resumable manner, in case the network goes down during a backup for a few minutes
    or hours :/, then the upload can resume; requiring not have to re-upload the pieces of the chunk; this
    hopefully can save precious bandwidth.
    """
    print("Beginning upload of chunk: {}, Chunk: {} Out of: {}.".format(
        file_chunk_name, chunk_num, num_chunks))
    # Check whether this chunk has been uploaded before
    file_status: ChangedFile = drive_chunks.check_if_chunk_exists_or_changed(
        file_chunk, file_chunk_name)
    # Upload the file_chunk to google drive
    request = None
    # Chunk has never been uploaded before
    if file_status.changed and not file_status.file_id:
        request = service.files().create(
            media_body=file_chunk, body={'name': file_chunk_name, 'parents': [drive_chunks.folder_id]})
    # Chunk has been uploaded before but it has been changed
    elif file_status.changed and file_status.file_id:
        request = service.files().update(
            media_body=file_chunk, fileId=file_status.file_id)
    # The chunk has not changed and does not need to be re-uploaded.
    else:
        print("Chunk: {} is already up to date!".format(file_chunk_name))
        return True
    # beginning back-off duration for if an error occurs and we try to resume
    backoff: IncreasingBackoff = IncreasingBackoff(0.5, 10 * (60), 2)
    response = None
    while response is None:
        # Attempt to upload a chunk of the file
        try:
            status, response = request.next_chunk()
        # Catch any errors that occur, attempting to continue
        # uploading if possible
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                # Call next chunk again, using increasing backoff
                print("An error occurred. Trying again with increasing backoff."
                      " Waiting: {} seconds.".format(backoff.wait_time))
                backoff.wait()
                response = None
                continue
            else:
                # Error and quit
                print("Fatal Error uploading chunk.")
                return False
        # Handle the internet connection going out while backing up the file
        except Exception:
            print('Connection timed out, attempting again in {} seconds.'.format(
                backoff.wait_time))
            backoff.wait()
            continue
        # Reset increasing backoff time amount
        backoff.reset_to_initial()
        if status:
            print("Chunk upload progress: {}%.".format(
                int(status.progress() * 100)))
    print("Upload of Chunk: {} Complete!".format(file_chunk_name))
    return True


def begin_backup(service, local_file_name: str, dest_folder_name: str,
                 file_chunk_size: int = 250, upload_chunk_size: int = 1) -> bool:
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
    # Create the DriveChunks Object
    drive_chunks: DriveChunks = DriveChunks(service, folder_id)
    # Unable to find or make a folder to back up the file to
    if folder_id is None:
        return False
    # Open up the file and start chunking
    with open(local_file_name, 'rb') as local_file:
        # Calculate the size of the file to backup
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
            # Initialize the IncreasingBackoff retry object, incase something goes wrong
            backoff: IncreasingBackoff = IncreasingBackoff(0.5, 10 * (60), 2)
            # Upload this chunk to google drive
            status: bool = False
            while status is False:
                # Attempt to upload the chunk
                status = backup_chunked_file_piece(
                    service, drive_chunks, file_chunk, dest_folder_name +
                    '.' + str(chunk_num), chunk_num, num_chunk_files)
                # If successful continue, otherwise wait and try again.
                if status:
                    backoff.reset_to_initial()
                    break
                print("Upload of this chunk failed in a non-resumable way. Re-attempting the upload "
                      "in {} seconds.".format(backoff.wait_time))
                backoff.wait()
            # record the number of bytes uploaded
            # and move the index over one to not re-upload the end index of the
            # previous chunk as the start index of the next.
            bytes_uploaded += file_chunk.size()
        print("Upload of: {} as {} was successful.".format(
            local_file_name, dest_folder_name))
        return True


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
    # Make sure all the required arguments are there
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
