# STL resources
import os.path
import pickle

# Google API libraries
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


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


def get_drive_service() -> object or None:
    """
    Acquire the required credentials using the function,
    and build the google drive service resource.
    """
    credentials = get_credentials()
    if credentials is None:
        print("Error. unable to acquire credentials.")
        return None
    return build('drive', 'v3', credentials=credentials)
