import time
import os
import fcntl
import errno


## Provides the simplest possible interface to flock-based file locking. Intended for use with the `with` syntax. It will create/truncate/delete the lock file as necessary.
class SimpleFlock:
    ## Initializes the file lock
    #  @param path The lock file location
    #  @param Timeout (optional) Timeout in seconds
    def __init__(self, path:str, timeout: float=None):
        self.__path = path
        self.__timeout = timeout
        self.__fd = None

    ## Enter section
    def __enter__(self):
        self.__fd = os.open(self.__path, os.O_CREAT)
        start_lock_search = time.monotonic()
        while True:
            try:
                fcntl.flock(self.__fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired!
                return
            except (OSError, IOError) as ex:
                if ex.errno != errno.EAGAIN:  # Resource temporarily unavailable
                    raise
                elif self.__timeout is not None and time.monotonic() > (start_lock_search + self.__timeout):
                    # Exceeded the user-specified timeout.
                    raise TimeoutError

            # TODO It would be nice to avoid an arbitrary sleep here, but spinning without a delay is also undesirable.
            time.sleep(0.1)

    ## Exit section
    def __exit__(self, *args):
        fcntl.flock(self.__fd, fcntl.LOCK_UN)
        os.close(self.__fd)
        self.__fd = None

        # Try to remove the lock file, but don't try too hard because it is
        # unnecessary. This is mostly to help the user see whether a lock
        # exists by examining the filesystem.
        try:
            os.unlink(self.__path)
        except:
            pass
