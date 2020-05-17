# STL resources
import os.path
# ECBU Modules
from Credentials import get_drive_service
from DriveAccessFuncs import find_or_create_backup_folder, get_chunk_file_ids
# Google API libraries
from googleapiclient.http import MediaIoBaseDownload


def begin_file_restore(service, backup_folder_name: str, local_file_name: str,
                       google_drive_chunk_size: int = 1) -> bool:
    # Get the folder id of the backup folder in google drive
    folder_id: str = find_or_create_backup_folder(
        service, backup_folder_name, False)
    # Make sure that the folder to restore from exists
    if folder_id is None:
        print("Folder does not exist in drive to restore " +
              local_file_name + " from.")
        return False
    # Get the ids for each of the chunks from google drive
    chunk_ids: list = get_chunk_file_ids(service, folder_id)
    # Open up the local file
    with open(local_file_name, 'ab+') as local_file:
        # Find out how big the local file is
        file_size: int = local_file.tell()
        # Check whether we already have pieces of the file
        if file_size > 0:
            # Find out what the chunk size is from google drive
            pass


def main():
    # Build the google drive service
    service = get_drive_service()
    backup_folder_name: str = 'HHS'
    local_file_name: str = 'HedgeHogStew.mp4'
    google_drive_chunk_size: int = 1
    # Begin pulling down the chunks from google drive
    begin_file_restore(service, backup_folder_name,
                       local_file_name, google_drive_chunk_size)


if __name__ == "__main__":
    main()
