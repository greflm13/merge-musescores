import os
import json
import gzip
import shutil
import logging
import datetime

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__)).removesuffix(__package__ if __package__ else "")
LOG_DIR = os.path.join(SCRIPTDIR, "logs")
LATEST_LOG_FILE = os.path.join(LOG_DIR, "latest.jsonl")
os.makedirs(LOG_DIR, exist_ok=True)


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "asctime": self.formatTime(record),
            "created": record.created,
            "filename": record.filename,
            "funcName": record.funcName,
            "levelname": record.levelname,
            "levelno": record.levelno,
            "lineno": record.lineno,
            "module": record.module,
            "msecs": record.msecs,
            "message": record.getMessage(),
            "process": record.process,
            "processName": record.processName,
            "relativeCreated": record.relativeCreated,
            "thread": record.thread,
            "threadName": record.threadName,
        }

        # fmt: off
        standard_fields = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
            "relativeCreated", "thread", "threadName", "processName", "process", "message",
            "asctime",
        }
        # fmt: on

        for key, value in record.__dict__.items():
            if key not in standard_fields:
                log_entry[key] = value

        return json.dumps(log_entry)


def rotate_log_file(compress=True) -> None:
    """
    Truncates the 'latest.jsonl' file after optionally compressing its contents to a timestamped file.
    The 'latest.jsonl' file is not deleted or moved, just emptied.

    Args:
        compress (bool): If True, compress the old log file using gzip.
    """
    if os.path.exists(LATEST_LOG_FILE):
        with open(LATEST_LOG_FILE, "r+", encoding="utf-8") as f:
            first_line = f.readline()
            try:
                first_log = json.loads(first_line)
                first_timestamp = first_log.get("asctime")
                first_timestamp = first_timestamp.split(",")[0]
            except (json.JSONDecodeError, KeyError):
                first_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            safe_timestamp = first_timestamp.replace(":", "-").replace(" ", "_")
            old_log_filename = os.path.join(LOG_DIR, f"{safe_timestamp}.jsonl")

            with open(old_log_filename, "w", encoding="utf-8") as old_log_file:
                f.seek(0)
                shutil.copyfileobj(f, old_log_file)

            if compress:
                with open(old_log_filename, "rb") as f_in:
                    with gzip.open(f"{old_log_filename}.gz", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(old_log_filename)

            f.seek(0)
            f.truncate()
