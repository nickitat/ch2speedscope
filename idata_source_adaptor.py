from typing import List, NamedTuple, Tuple


"""
This class provides all the data required to build an event-based profile for SpeedScope.
It is less abstract that the format itself, but seemingly is still general enough for all trace types supported by ClickHouse.
You could find the spec here: https://github.com/jlfwong/speedscope/blob/main/src/lib/file-format-spec.ts
"""


class IDataSourceAdaptor(object):
    Event = NamedTuple("Event", [("trace", int), ("at", int)])

    def __init__(self, query_id, host, port, user, pwd, secure, on_cluster):
        self.query_id = query_id
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.secure = secure
        self.on_cluster = on_cluster

    """
    Returns the unit in which startValue and endValue are measured
    """

    def get_value_unit(self) -> str:
        return "none"

    """
    Returns startValue
    """

    def get_start_value(self) -> int:
        raise NotImplementedError()

    """
    Returns endValue
    """

    def get_end_value(self) -> int:
        raise NotImplementedError()

    """
    Returns the list of all profile's frames
    """

    def get_frames(self) -> List:
        raise NotImplementedError()

    """
    Returns the list of demangled `frames`.
    E.g. if `frames` is a list of function addresses - returns the list of corresponding function names.
    Default implementation will return `frames` as is.
    """
    # TODO: consider merging with get_frames
    def get_demangled_frames(self, frames) -> List[str]:
        return frames

    """
    The whole file can contain multiple profiles - each for it's own thread.
    Each thread can have an id and a name. The resulting profile will have the name matching the thread name.
    The `get_events` method will be called for each pair and passed in arguments.
    Returns list of pairs (thread id, thread name)
    """

    def get_threads(self) -> List[Tuple[int | str, int | str]]:
        raise NotImplementedError

    """
    Returns the list of events for the specified thread.
    Events should go in order of increasing `at` values, frames are numerated starting from 0.
    """

    def get_events_for_thread(self, frames, thread_id, thread_name) -> List[Event]:
        raise NotImplementedError
