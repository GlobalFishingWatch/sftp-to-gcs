import os
import logging
from typing import Optional, Any, Callable

import asyncio
import asyncssh
import aiohttp

from cloudpathlib import GSPath
from gcloud.aio.storage import Storage
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

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


def _is_transient(exc: Exception) -> bool:
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status >= 500

    if isinstance(exc, asyncssh.SFTPError):
        return exc.code not in (
            asyncssh.FX_NO_SUCH_FILE,
            asyncssh.FX_PERMISSION_DENIED,
        )

    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
async def _process_file(
    sftp,
    filename: str,
    sftp_directory: str,
    gcs_client: Storage,
    day_folder: GSPath,
    semaphore: asyncio.Semaphore,
    chunk_size: int,
) -> None:
    async with semaphore:
        logger.info("Processing %s", filename)
        remote_path = f"{sftp_directory}/{filename}"
        queue = asyncio.Queue(maxsize=5)

        producer_task = asyncio.create_task(
            _producer(sftp, remote_path, queue, chunk_size)
        )
        try:
            await _consumer(queue, gcs_client, day_folder, filename)
        except (asyncssh.SFTPError, aiohttp.ClientResponseError):
            producer_task.cancel()
            raise

        await producer_task
        # await _write_success_file(gcs_client, day_folder, filename)
        logger.info("Finished processing %s", filename)


async def _producer(
    sftp,
    remote_path: str,
    queue: asyncio.Queue,
    chunk_size: int,
) -> None:
    logger.info("Reading file %s", remote_path)
    chunk = []
    try:
        async with sftp.open(remote_path, "rb") as f:
            buffer = b""
            while True:
                data = await f.read(65536)  # 64KB at a time
                if not data:
                    break

                buffer += data
                lines = buffer.split(b"\n")
                buffer = lines.pop()  # keep incomplete last line

                for raw_line in lines:
                    line = raw_line.decode("utf-8").strip()
                    if not line:
                        continue

                    chunk.append(line)
                    if len(chunk) == chunk_size:
                        await queue.put(chunk)
                        chunk = []

            # handle remaining buffer
            if buffer:
                line = buffer.decode("utf-8").strip()
                if line:
                    chunk.append(line)
        if chunk:
            await queue.put(chunk)
    except (asyncssh.SFTPError, asyncssh.SFTPNoSuchFile, UnicodeDecodeError):
        logger.exception("Error reading file %s", remote_path)
        raise
    finally:
        await queue.put(None)


async def _consumer(
    queue: asyncio.Queue,
    gcs_client: Storage,
    day_folder: GSPath,
    filename: str,
) -> None:
    while True:
        chunk = await queue.get()
        if chunk is None:
            break
        # TODO: serialize to AVRO and write to GCS (next ticket)
        logger.info("Received chunk of %d lines from %s", len(chunk), filename)


async def run(
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
    sftp_factory: Callable = asyncssh.connect,
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
    day_folders = {f: _gcs_day_folder(base_path, f) for f in filenames}

    async with storage_factory() as gcs_client:
        results = await asyncio.gather(*[
            _success_file_exists(gcs_client, day_folders[f], f)
            for f in filenames
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

    logger.info("Connecting to the SFTP...")
    semaphore = asyncio.Semaphore(concurrency)

    async with sftp_factory(
        sftp_host,
        port=sftp_port,
        username=sftp_user,
        password=sftp_pass,
        known_hosts=None,
    ) as conn:
        async with conn.start_sftp_client() as sftp:
            async with storage_factory() as gcs_client:
                tasks = [
                    asyncio.create_task(
                        _process_file(
                            sftp=sftp,
                            filename=filename,
                            sftp_directory=sftp_directory,
                            gcs_client=gcs_client,
                            day_folder=day_folders[filename],
                            semaphore=semaphore,
                            chunk_size=chunk_size,
                        )
                    )
                    for filename in filenames_to_process
                ]
                await asyncio.gather(*tasks)
