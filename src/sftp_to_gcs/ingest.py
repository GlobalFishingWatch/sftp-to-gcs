import logging
import asyncio  # noqa
from typing import Optional, Any

from cloudpathlib import GSPath

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SFTP_FILENAME_FORMAT = "ais-%Y-%m-%d-%H-%M.nmea"


def _generate_filenames(datetime_from: datetime, datetime_to: datetime) -> list[str]:
    filenames = []
    current = datetime_from
    while current < datetime_to:
        filenames.append(current.strftime(SFTP_FILENAME_FORMAT))
        current += timedelta(minutes=5)
    return filenames


async def run(
    project: str,
    sftp_host: str,
    sftp_port: int,
    sftp_user: str,
    sftp_directory: str,
    sftp_pass_env: str,
    datetime_from: str,
    datetime_to: str,
    gcs_path: GSPath,
    chunk_size: int = 12_500,
    concurrency: int = 20,
    unknown_unparsed_args: Optional[list[str]] = None,
    unknown_parsed_args: Optional[dict[str, Any]] = None,
) -> None:
    gcs_path = GSPath(gcs_path)

    datetime_from_dt = datetime.fromisoformat(datetime_from)
    datetime_to_dt = datetime.fromisoformat(datetime_to)

    filenames = _generate_filenames(datetime_from_dt, datetime_to_dt)
    logger.info("Generated %d filenames to process", len(filenames))
