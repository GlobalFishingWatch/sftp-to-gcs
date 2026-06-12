import sys
import logging
from typing import Any

import asyncio

from gfw.common.cli import CLI
from gfw.common.cli.command import Option
from gfw.common.logging import LoggerConfig
from gfw.common.cli.formatting import default_formatter

from sftp_to_gcs.version import __version__
from sftp_to_gcs import ingest


logger = logging.getLogger(__name__)

HELP_SFTP_HOST = "SFTP server hostname."
HELP_SFTP_PORT = "SFTP server port."
HELP_SFTP_USER = "SFTP username."
HELP_SFTP_PASS_ENV = "Name of the environment variable containing the SFTP password."
HELP_SFTP_PASS_PATH = "Path to a file containing the sFTP password (Kubernetes secret)."
HELP_SFTP_DIRECTORY = "Remote SFTP directory to read files from."
HELP_SFTP_FILENAME = "SFTP filename format (e.g. ais-%%Y-%%m-%%d-%%H-%%M.nmea)."
HELP_DATETIME_FROM = "Start datetime of the range to process (inclusive), format YYYY-MM-DDTHH:MM."
HELP_DATETIME_TO = "End datetime of the range to process (exclusive), format YYYY-MM-DDTHH:MM."
HELP_SOURCE_NAME = "Source name to include in AVRO record attributes."
HELP_GCS_PATH = "Destination GCS path where AVRO files will be written (e.g. gs://bucket/dir/)."
HELP_GCS_RECORD_SIZE = "Number of sentences per AVRO record."
HELP_BUFFER_SIZE = "Number of lines buffered before flushing to GCS. Controls memory usage."
HELP_CONCURRENCY = "Maximum number of SFTP files processed concurrently."


def run(args: list, **kwargs: Any) -> Any:
    sftp_to_gcs_cli = CLI(
        name="sftp-to-gcs",
        description=(
            "A CLI tool that transfers files from an SFTP server to Google Cloud Storage."
        ),
        formatter=default_formatter(max_pos=100),
        options=[
            Option("--sftp-host", type=str, required=True, help=HELP_SFTP_HOST),
            Option("--sftp-port", type=int, default=22, help=HELP_SFTP_PORT),
            Option("--sftp-user", type=str, required=True, help=HELP_SFTP_USER),
            Option("--sftp-pass-env", type=str, required=False, help=HELP_SFTP_PASS_ENV),
            Option("--sftp-pass-path", type=str, required=False, help=HELP_SFTP_PASS_PATH),
            Option("--sftp-directory", type=str, required=True, help=HELP_SFTP_DIRECTORY),
            Option("--sftp-filename-format", type=str, required=True, help=HELP_SFTP_FILENAME),
            Option("--datetime-from", type=str, required=True, help=HELP_DATETIME_FROM),
            Option("--datetime-to", type=str, required=True, help=HELP_DATETIME_TO),
            Option("--source-name", type=str, required=True, help=HELP_SOURCE_NAME),
            Option("--gcs-path", type=str, required=True, help=HELP_GCS_PATH),
            Option("--gcs-record-size", type=int, default=20, help=HELP_GCS_RECORD_SIZE),
            Option("--buffer-size", type=int, default=250_000, help=HELP_BUFFER_SIZE),
            Option("--concurrency", type=int, default=20, help=HELP_CONCURRENCY),
        ],
        subcommands=[],
        version=__version__,
        examples=[
            "sftp_to_gcs -h",
            "sftp_to_gcs -c config/example.yaml",
        ],
        logger_config=LoggerConfig(
            warning_level=[]
        ),
        allow_unknown=False,
        run=lambda config, **kwargs: asyncio.run(ingest.run(config, **kwargs)),

    )

    return sftp_to_gcs_cli.execute(args, **kwargs)


def main():
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
