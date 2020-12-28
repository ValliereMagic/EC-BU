# STL resources
import os
from io import BufferedReader

# Google API libraries
from googleapiclient.http import MediaUpload


class InvalidChunkException(ValueError):
    pass


class OffsetOutOfBoundsException(ValueError):
    pass


class ECBUMediaUpload(MediaUpload):
    """
    This class represents a piece of a file to be uploaded as a file.
    It wraps a BufferedReader for a whole file, but only allows it to read
    starting at begin_offset until it reaches end_offset.

    file_descriptor: file descriptor, returned from an open statement.
    file_size: the length of the file in bytes.
    begin_index: the beginning index of the file that makes up this upload.
    end_index: the end index of the file that makes up this upload.
    chunk size: the amount of the file pulled into memory and uploaded at a time.
    resumable: whether this upload uses the resumable feature of
        google drive. We want this.
    """

    def __init__(self, file_descriptor: BufferedReader, file_size: int, begin_index: int,
                 end_index: int, chunk_size: int = 1 * (1024 * 1024), resumable: bool = True):
        self._file_descriptor = file_descriptor
        self._mimetype = "application/octet-stream"
        # check if we are within the bounds of the file
        if begin_index > file_size or end_index > file_size \
                or begin_index < 0 or end_index < 0:
            raise OffsetOutOfBoundsException(
                'One of the offsets provided is outside the length of the file.')
        self._begin_index = begin_index
        self._end_index = end_index
        # Make sure that the chunk size makes sense
        if chunk_size < 0:
            raise InvalidChunkException(
                'The chunk size must be greater than zero.')
        self._chunk_size = chunk_size
        self._resumable = resumable

    def chunksize(self) -> int:
        return self._chunk_size

    def mimetype(self) -> str:
        return self._mimetype

    def size(self) -> int:
        return (self._end_index - self._begin_index)

    def resumable(self) -> bool:
        return self._resumable

    def getbytes(self, begin: int, length: int) -> bytes:
        """
        Move to the beginning of this offset into our file
        by calculating with the begin offset,
        and then read the length requested.
        Unless it goes off the end. Then shorten the length
        to only go to to end_index
        """
        read_start_index: int = begin + self._begin_index
        self._file_descriptor.seek(read_start_index)
        # Make sure we don't go out of bounds of our
        # segment of the file
        if (read_start_index + length) > self._end_index:
            length = (self._end_index - read_start_index)
        return self._file_descriptor.read(length)

    def has_stream(self) -> bool:
        # We don't want it to use this interface. We want it to
        # use getbytes
        return False

    def stream(self):
        raise NotImplementedError(
            "Please don't try to access the internal stream.")

    def to_json(self):
        raise NotImplementedError('This class is not serializable.')
