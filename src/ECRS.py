# STL resources
import os.path
from argparse import ArgumentParser, Namespace
# ECBU Modules
from Credentials import get_drive_service
from CommandLineParse import parse_integer_argument
from DriveAccessFuncs import find_or_create_backup_folder, get_chunk_file_information
from ChunkChanges import check_if_chunk_exists_or_changed, ChangedFile
from UploadAbstraction import ECBUMediaUpload
# Google API libraries
from googleapiclient.http import MediaIoBaseDownload


def download_chunk(service, local_file, bytes_downloaded: int,
                   download_chunk_size: int, chunk: dict):
    """
    Using the drive service, download the passed chunk by id
    bytes_downloaded bytes into the file.
    """
    # Seek to the spot to put the chunk
    local_file.seek(bytes_downloaded)
    # Download the chunk at this spot
    print("Beginning download of chunk: " + chunk['name'] + ".")
    request = service.files().get_media(fileId=chunk['id'])
    chunk_downloader = MediaIoBaseDownload(
        local_file, request, download_chunk_size * (1024 * 1024))
    # Download each chunk of the chunk, reporting progress along
    # the way.
    completed: bool = False
    while not completed:
        status, completed = chunk_downloader.next_chunk(10)
        if status:
            print("Downloaded %d%%." %
                  int(status.progress() * 100))
    print("Download of chunk: " +
          chunk['name'] + " completed!")


def begin_file_restore(service, backup_folder_name: str, local_file_name: str,
                       download_chunk_size: int = 1) -> bool:
    """
    Get the ids, names, and sizes of each of the chunks representing the local_file
    within the folder with backup_folder_name on google drive
    Then, open up the local verson of the backup, creating it if it doesn't exist
    and see how much of the file to restore we already have.
    Move through the file, hashing the chunks as we go, downloading changed and
    new chunks, and leaving chunks that are already up to date with the backup.
    """
    # Get the folder id of the backup folder in google drive
    folder_id: str = find_or_create_backup_folder(
        service, backup_folder_name, False)
    # Make sure that the folder to restore from exists
    if folder_id is None:
        print("Folder does not exist in drive to restore " +
              local_file_name + " from.")
        return False
    # Get the required information for each of the chunks in google drive
    # Dictionary containing the id, name, and size of each chunk
    chunk_information: list = get_chunk_file_information(service, folder_id)
    if not chunk_information:
        return False
    # Open up the local file
    with open(local_file_name, 'ab+') as local_file:
        # Find out how big the local file is
        file_size: int = local_file.tell()
        # Go through each chunk, tallying how many bytes we already have
        # as we go.
        bytes_downloaded: int = 0
        for chunk in chunk_information:
            chunk_size: int = int(chunk['size'])
            # Check if we already downloaded this chunk
            if file_size >= (chunk_size + bytes_downloaded):
                # Check if the chunk has been changed
                chunk_representation: ECBUMediaUpload = ECBUMediaUpload(
                    local_file, file_size, bytes_downloaded, bytes_downloaded + chunk_size)
                result: ChangedFile = check_if_chunk_exists_or_changed(
                    service, chunk_representation, folder_id, chunk['name'])
                # The chunk has been changed and needs to be re-downloaded in this spot.
                if result.changed and result.ident:
                    download_chunk(service, local_file,
                                   bytes_downloaded, download_chunk_size, chunk)
                # The chunk somehow doesn't exist. Fatal error. Exit.
                elif result.changed and not result.ident:
                    print("Error. Chunk: " +
                          chunk['name' + " somehow doesn't exist in backup."])
                    return False
                # The chunk is the same, and doesn't need to be re-downloaded.
                else:
                    print("Chunk: " + chunk['name'] +
                          " is already up to date!")
                    bytes_downloaded += chunk_size
            # We don't have this chunk yet in our local copy.
            else:
                download_chunk(service, local_file,
                               bytes_downloaded, download_chunk_size, chunk)
    return True


def main():
    """
    Grab the CLI arguments passed by the user, and then begin a restore of
    the local file.
    """
    # Register the argument parser
    arg_parser: ArgumentParser = ArgumentParser(
        description="EC-RS ~Exposed Conscious Re-Store~")
    # Register the two different argument groups
    required = arg_parser.add_argument_group("required arguments")
    required.add_argument('--backup-folder-name', dest="backup_folder_name",
                          help="The name of the folder in google drive "
                          "containing the backed up file chunks")
    required.add_argument('--local-file-name', dest="local_file_name",
                          help="The name of the file the backup "
                          "is to be restored to locally.")
    arg_parser.add_argument('--download-chunk-size', dest="download_chunk_size",
                            help="The chunk size for downloading chunks. "
                            "(how many pieces to download each chunk in) (Mebibytes)")
    # Parse the arguments entered by the user
    parsed_args: Namespace = arg_parser.parse_args()
    # Make sure all the required arguments are there
    if parsed_args.backup_folder_name is None or \
       parsed_args.local_file_name is None:
        arg_parser.print_help()
        return
    # Build the google drive service
    service = get_drive_service()
    # Check if the optional argument was passed and
    # begin pulling down the chunks from google drive
    if parsed_args.download_chunk_size:
        # Try and convert the optional arg to an int
        download_chunk_size: int = parse_integer_argument(parsed_args.download_chunk_size,
                                                          "Error. Download chunk size "
                                                          "must be an integer.")
        if download_chunk_size is None:
            return
        begin_file_restore(service, parsed_args.backup_folder_name,
                           parsed_args.local_file_name, download_chunk_size)
    else:
        begin_file_restore(service, parsed_args.backup_folder_name,
                           parsed_args.local_file_name)


if __name__ == "__main__":
    main()
