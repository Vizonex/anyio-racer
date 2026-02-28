class RacerError(Exception):
    """An Exception type raised by `anyio-racer`"""


class RaceFailedError(RacerError):
    """When No Task succeeded this exception gets raised
    when no task is seen as finished"""


__all__ = (
    "RaceFailedError",
    "RacerError",
)

