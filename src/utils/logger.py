import logging
from typing import TYPE_CHECKING, Any, MutableMapping

if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:
    _LoggerAdapter = logging.LoggerAdapter


class CustomLoggingAdapter(_LoggerAdapter):
    """A logging adapter that adds some context
    before the log message."""

    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        assert self.extra is not None
        return (f"[{self.extra['ctx']}] {msg}", kwargs)
