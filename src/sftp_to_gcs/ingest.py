import logging
import asyncio  # noqa
import aiohttp
import os

from typing import Optional, Any, Callable

from cloudpathlib import GSPath
from gcloud.aio.storage import Storage

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SFTP_FILENAME_FORMAT = "ais-%Y-%m-%d-%H-%M.nmea"
SUCCESS_FILE_PREFIX = "_SUCCESS_"


def _generate_filenames(datetime_from: datetime, datetime_to: datetime) -> list[str]:
    filenames = []
    current = datetime_from
    while current < datetime_to:
        filenames.append(current.strftime(SFTP_FILENAME_FORMAT))
        current += timedelta(minutes=5)
    return filenames


def _build_base_path(gcs_path: GSPath, datetime_from: datetime, datetime_to: datetime) -> GSPath:
    range_folder = f"{datetime_from.isoformat()}--{datetime_to.isoformat()}"
    return gcs_path / range_folder


def _gcs_day_folder(base_path: GSPath, filename: str) -> GSPath:
    # ais-2026-05-11-20-50.nmea → 2026-05-11
    file_dt = datetime.strptime(filename, SFTP_FILENAME_FORMAT)
    day_folder = f"nmea-{file_dt.strftime('%Y-%m-%d')}"
    return base_path / day_folder


async def _success_file_exists(gcs_client: Storage, day_folder: GSPath, filename: str) -> bool:
    success_path = day_folder / f"{SUCCESS_FILE_PREFIX}{filename}"
    try:
        await gcs_client.download_metadata(
            success_path.bucket,
            success_path.blob,
        )
        return True
    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            return False
        raise


async def run(
    project: str,
    sftp_host: str,
    sftp_port: int,
    sftp_user: str,
    sftp_directory: str,
    sftp_pass_env: str,
    datetime_from: str,
    datetime_to: str,
    gcs_path: str,
    chunk_size: int = 12_500,
    concurrency: int = 20,
    storage_factory: Callable = Storage,
    unknown_unparsed_args: Optional[list[str]] = None,
    unknown_parsed_args: Optional[dict[str, Any]] = None,
) -> None:
    gcs_path = GSPath(gcs_path)

    datetime_from_dt = datetime.fromisoformat(datetime_from)
    datetime_to_dt = datetime.fromisoformat(datetime_to)

    logger.info(
        "Generating all filenames in the range: [{}, {}]...".format(datetime_from, datetime_to))

    filenames = _generate_filenames(datetime_from_dt, datetime_to_dt)
    logger.info("Generated %d filenames to process", len(filenames))

    sftp_pass = os.environ.get(sftp_pass_env)
    if not sftp_pass:
        raise ValueError(f"'{sftp_pass_env}' environment variable is not set")

    logger.info("Checking for SUCCESS files existence...")
    base_path = _build_base_path(gcs_path, datetime_from_dt, datetime_to_dt)
    day_folders = [_gcs_day_folder(base_path, f) for f in filenames]

    async with storage_factory(project=project) as gcs_client:
        results = await asyncio.gather(*[
            _success_file_exists(gcs_client, day_folder, f)
            for f, day_folder in zip(filenames, day_folders)
        ])
        filenames_to_process = [f for f, exists in zip(filenames, results) if not exists]

    logger.info(
        "%d files to process, %d already complete",
        len(filenames_to_process),
        len(filenames) - len(filenames_to_process),
    )

    if not filenames_to_process:
        logger.info("Nothing to do, exiting")
        return

    raise NotImplementedError
