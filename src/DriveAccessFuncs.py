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
    """
    return item.get('name')


def get_chunk_file_ids(service, folder_id: str) -> list:
    folder_ids = list()
    page_token = None
    while True:
        response = service.files().list(q="'" + folder_id + "' in parents and trashed = false",
                                        spaces='drive', fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        # sort the response to make sure it is in order
        # eg. HHS.1, HHS.2 ... HHS.500
        sorted_respones: list = response.get('files', [])
        sorted_respones.sort(key=chunk_id_response_compare)
        # Append the ids of the chunks in sorted order.
        for file in sorted_respones:
            folder_ids.append(file.get('id'))
        # Move on to the next page
        page_token = response.get('nextPageToken', None)
        # No more pages to look through
        if page_token is None:
            break
    return folder_ids
