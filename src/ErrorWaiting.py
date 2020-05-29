import time


class IncreasingBackoff:

    """
    Calling wait will increasingly wait longer and longer until
    max_wait_time is reached. Time values passed are in seconds
    """

    def __init__(self, initial_time_secs: float, max_wait_time: float,
                 stepping_multiplier: int):
        # Set initial values
        self._initial_wait_time: float = initial_time_secs
        self.wait_time: float = initial_time_secs
        self._max_wait_time: float = max_wait_time
        self._stepping_multiplier: int = stepping_multiplier

    def wait(self):
        # Sleep for the stored amount of time
        time.sleep(self.wait_time)
        # Increase the wait time
        if self.wait_time < self._max_wait_time:
            wait_time_increase: float = self.wait_time * self._stepping_multiplier
            # Only multiply if the wait time increase will not
            # exceed the max wait time.
            if wait_time_increase > self._max_wait_time:
                self.wait_time = self._max_wait_time
                return
            self.wait_time = wait_time_increase

    def reset_to_initial(self):
        # Reset the increasing backoff back to the start again
        self.wait_time = self._initial_wait_time
