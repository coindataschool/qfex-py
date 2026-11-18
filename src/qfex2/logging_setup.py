"""
Shared logging setup for qfex2-based strategy scripts.

Lives here rather than inside QFEXConfig because logging.basicConfig()
configures the process-wide root logger, not something scoped to one
client instance — if it lived in QFEXConfig, running two clients in one
process would hit basicConfig()'s "first call wins" behavior and silently
ignore the second client's logging preferences.

Usage, at the top of a strategy script's main():

    from qfex2.logging_setup import setup_logging
    setup_logging(args.log_level, log_file="logs/maker.log")
"""

import logging
import logging.handlers
from typing import Optional

_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    backup_days: int = 14,
    console: bool = True,
) -> None:
    """
    Configures the root logger once, for the whole process. Call this
    near the start of your script's main(), before constructing any
    client, so their first log lines are visible.

    `level` is a case-insensitive string like "DEBUG"/"INFO"/"WARNING";
    falls back to INFO rather than raising if unrecognized.

    `console` controls terminal output (stderr), on by default. Set False
    to suppress it — e.g. for a background run where only the file log
    matters.

    `log_file`, if given, also logs there, rotating daily at midnight and
    keeping `backup_days` of history (default 14) — unlike size-based
    rotation, this doesn't cap total disk usage, so watch it directly if
    that's a concern. The parent directory is not created automatically;
    a missing one raises FileNotFoundError rather than silently creating
    arbitrary directories.

    If `console=False` and `log_file` is None, no handler is attached at
    all and every log call becomes a silent no-op — this prints one
    warning line directly to stderr (not through logging, which has no
    handler yet) before proceeding, rather than raising.
    """
    if not console and log_file is None:
        import sys
        print(
            "qfex2.setup_logging: console=False and log_file=None — no "
            "log handler will be attached; all logging in this process "
            "will be silently discarded",
            file=sys.stderr,
        )

    resolved = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter(fmt=_FORMAT, datefmt=_DATEFMT)

    root = logging.getLogger()
    root.setLevel(resolved)
    # Clear any handlers from a prior setup_logging() call in this process
    # (e.g. tests, or a script that calls this more than once) so levels/
    # handlers don't silently accumulate duplicates.
    root.handlers.clear()

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    if log_file is not None:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=backup_days
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
