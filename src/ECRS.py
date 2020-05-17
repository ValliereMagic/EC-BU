# ECBU Modules
from Credentials import get_drive_service
# Google API libraries
from googleapiclient.http import MediaIoBaseDownload


def main():
    # Build the google drive service
    service = get_drive_service()


if __name__ == "__main__":
    main()
