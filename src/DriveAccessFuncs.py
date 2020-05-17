# STL resources
import re


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


def chunk_id_response_compare(item: dict):
    """
    Key function for sorting responses of
    files within a backup folder.

    Remove everything from the file name up to and
    including the '.', then convert it to an integer.
    i.e. isolate the 23 in HHS.23
    """
    return int(re.sub(r'^(.*)\.', '', item.get('name')))


def get_chunk_file_information(service, folder_id: str) -> list:
    """
    Query google drive and acquire the ids in order of each chunk making
    up the file to restore.
    """
    chunk_file_information = list()
    page_token = None
    while True:
        response = service.files().list(q="'" + folder_id + "' in parents and trashed = false",
                                        spaces='drive', fields='nextPageToken, files(id, name, size)',
                                        pageToken=page_token).execute()
        # Append the chunk information.
        chunk_file_information += response.get('files', [])
        # Move on to the next page
        page_token = response.get('nextPageToken', None)
        # No more pages to look through
        if page_token is None:
            break
    # Sort the chunk_information so that we restore the folder in the correct order
    chunk_file_information.sort(key=chunk_id_response_compare)
    return chunk_file_information
