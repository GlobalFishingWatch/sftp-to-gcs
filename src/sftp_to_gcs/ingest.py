from __future__ import annotations
import os
import logging
from typing import Callable, Any
from types import SimpleNamespace
from datetime import timezone

import asyncio
import asyncssh
import aiohttp

from cloudpathlib import GSPath
from gcloud.aio.storage import Storage
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from datetime import datetime, timedelta

from sftp_to_gcs.avro import build_record, serialize_chunk, output_filename

logger = logging.getLogger(__name__)

SUCCESS_FILE_PREFIX = "_SUCCESS_"


def _build_base_path(gcs_path: GSPath, datetime_from: datetime, datetime_to: datetime) -> GSPath:
    range_folder = f"{datetime_from.isoformat()}--{datetime_to.isoformat()}"
    return gcs_path / range_folder


async def run(namespace: SimpleNamespace, **kwargs: Any) -> None:
    ingester = SftpToGcsIngester.from_namespace(namespace, **kwargs)
    await ingester.run(namespace.datetime_from, namespace.datetime_to)


def load_sftp_password(sftp_pass_path: str | None, sftp_pass_env: str | None) -> str:
    """Load the SFTP password from a secret file or an environment variable.

    Tries the file first (intended for production, where a Kubernetes init container
    writes the secret to a mounted volume). Falls back to the environment variable
    if the file is not provided or does not exist (intended for local development).

    Args:
        sftp_pass_path:
            Path to a file containing the password. Takes precedence over
            ``sftp_pass_env`` when the file exists.

        sftp_pass_env:
            Name of the environment variable containing the password.

    Returns:
        The SFTP password as a stripped string.

    Raises:
        ValueError: If the file exists but is empty, if the environment variable
            is not set, or if neither source is provided.
    """
    if sftp_pass_path is not None and os.path.exists(sftp_pass_path):
        logger.info(f"Reading sFTP password from secret '{sftp_pass_path}'...")
        # In production — file must exist AND have content
        with open(sftp_pass_path) as f:
            sftp_pass = f.read().strip()

        if not sftp_pass:
            raise ValueError(
                f"Secret file '{sftp_pass_path}' exists but is empty. "
                f"Check the init container fetched the secret correctly."
            )

        return sftp_pass

    if sftp_pass_env is not None:
        logger.info(f"Reading sFTP password from ENV variable '{sftp_pass_env}'...")
        # Local dev — fall back to env var
        sftp_pass = os.environ.get(sftp_pass_env)
        if not sftp_pass:
            raise ValueError(
                f"Environment variable '{sftp_pass_env}' not found. "
            )

        return sftp_pass

    raise ValueError(
        "No sFTP password source provided: supply a secret file path or an env variable name.")


class SftpToGcsIngester:
    """Downloads files from an SFTP server and writes them as chunked AVRO files to GCS.

    Files are expected to be newline-delimited, streamed line by line to avoid loading
    entire files into memory, and split into chunks of a configurable size.
    Each chunk is written as a separate AVRO file to a GCS path structured as:

        {gcs_path}/{datetime_from}--{datetime_to}/{date}/{time}_{uuid}.avro

    Resumability is supported via success files written to GCS after each SFTP
    file is fully processed. On restart, already-completed files are skipped.

    Args:
        sftp_host:
            SFTP server hostname.

        sftp_port:
            SFTP server port.

        sftp_user:
            SFTP username.

        sftp_pass:
            SFTP password.

        sftp_directory:
            Remote SFTP directory to read files from.

        sftp_filename_format:
            SFTP filename format using strptime directives
            (e.g. ``ais-%Y-%m-%d-%H-%M.nmea``).

        gcs_path:
            Destination GCS base path (e.g. ``gs://bucket/dir/``).

        chunk_size:
            Number of lines per output AVRO file.

        concurrency:
            Maximum number of SFTP files processed concurrently.

        storage_factory:
            Factory callable for creating a GCS Storage client.
            Defaults to :class:`~gcloud.aio.storage.Storage`. Override for testing.

        sftp_factory:
            Factory callable for creating an SFTP connection.
            Defaults to :func:`asyncssh.connect`. Override for testing.
    """
    def __init__(
        self,
        sftp_host: str,
        sftp_port: int,
        sftp_user: str,
        sftp_pass: str,
        sftp_directory: str,
        sftp_filename_format: str,
        source_name: str,
        gcs_path: GSPath,
        gcs_record_size: int = 20,
        chunk_size: int = 12500,
        concurrency: int = 20,
        storage_factory: Callable = Storage,
        sftp_factory: Callable = asyncssh.connect,
    ):
        self._sftp_host = sftp_host
        self._sftp_port = sftp_port
        self._sftp_user = sftp_user
        self._sftp_pass = sftp_pass
        self._sftp_directory = sftp_directory
        self._sftp_filename_format = sftp_filename_format
        self._source_name = source_name
        self._gcs_path = gcs_path
        self._gcs_record_size = gcs_record_size
        self._chunk_size = chunk_size
        self._semaphore = asyncio.Semaphore(concurrency)
        self._storage_factory = storage_factory
        self._sftp_factory = sftp_factory

    @classmethod
    def from_namespace(cls, namespace: SimpleNamespace, **kwargs: Any) -> SftpToGcsIngester:
        sftp_pass_path = namespace.sftp_pass_path
        sftp_pass_env = namespace.sftp_pass_env

        sftp_pass = load_sftp_password(sftp_pass_path, sftp_pass_env)

        return cls(
            sftp_host=namespace.sftp_host,
            sftp_port=namespace.sftp_port,
            sftp_user=namespace.sftp_user,
            sftp_pass=sftp_pass,
            sftp_directory=namespace.sftp_directory,
            sftp_filename_format=namespace.sftp_filename_format,
            source_name=namespace.source_name,
            gcs_path=GSPath(namespace.gcs_path),
            gcs_record_size=namespace.gcs_record_size,
            chunk_size=namespace.chunk_size,
            concurrency=namespace.concurrency,
            **kwargs,
        )

    async def run(self, datetime_from: str, datetime_to: str) -> None:
        datetime_from_dt = datetime.fromisoformat(datetime_from)
        datetime_to_dt = datetime.fromisoformat(datetime_to)

        logger.info("Generating filenames in range [%s, %s]...", datetime_from, datetime_to)
        filenames = self._generate_filenames(datetime_from_dt, datetime_to_dt)
        logger.info("Generated %d filenames to process", len(filenames))

        base_path = _build_base_path(self._gcs_path, datetime_from_dt, datetime_to_dt)
        day_folders = {f: self._gcs_day_folder(base_path, f) for f in filenames}

        logger.info("Checking for SUCCESS files...")
        async with self._storage_factory() as gcs_client:
            results = await asyncio.gather(*[
                self._success_file_exists(gcs_client, day_folders[f], f)
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

        async with self._sftp_factory(
            self._sftp_host,
            port=self._sftp_port,
            username=self._sftp_user,
            password=self._sftp_pass,
            known_hosts=None,
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                async with self._storage_factory() as gcs_client:
                    # TODO: refactor to a session class holding runtime state gcs_client.
                    # So we don't have to pass around the instance or do this.
                    self._gcs_client = gcs_client
                    async with asyncio.TaskGroup() as tg:
                        for filename in filenames_to_process:
                            tg.create_task(
                                self._process_file(
                                    sftp=sftp,
                                    gcs_client=gcs_client,
                                    filename=filename,
                                    day_folder=day_folders[filename],
                                )
                            )

    async def _success_file_exists(
        self, gcs_client: Storage, day_folder: GSPath, filename: str
    ) -> bool:
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

    def _is_transient(self, exc: Exception) -> bool:
        if isinstance(exc, aiohttp.ClientResponseError):
            return exc.status >= 500

        if isinstance(exc, asyncssh.SFTPError):
            return exc.code not in (
                asyncssh.FX_NO_SUCH_FILE,
                asyncssh.FX_PERMISSION_DENIED,
            )

        if isinstance(exc, ConnectionResetError):
            return True

        return False

    async def _process_file(
        self,
        sftp,
        gcs_client: Storage,
        filename: str,
        day_folder: GSPath,
    ) -> None:
        @retry(
            retry=retry_if_exception(self._is_transient),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
        )
        async def _run():
            async with self._semaphore:
                logger.info("Processing %s", filename)
                remote_path = f"{self._sftp_directory}/{filename}"
                publish_time = datetime.strptime(
                    filename, self._sftp_filename_format
                ).replace(tzinfo=timezone.utc)

                queue = asyncio.Queue(maxsize=5)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._producer(sftp, remote_path, queue))
                    tg.create_task(self._consumer(queue, day_folder, filename, publish_time))

                await self._write_success_file(day_folder, filename)
                logger.info("Finished processing %s", filename)

        await _run()

    async def _producer(self, sftp, remote_path: str, queue: asyncio.Queue) -> None:
        logger.info("Reading file %s", remote_path)
        chunk = []
        try:
            async with sftp.open(remote_path, "rb") as f:
                buffer = b""
                while True:
                    data = await f.read(65536)
                    if not data:
                        break

                    buffer += data
                    lines = buffer.split(b"\n")
                    buffer = lines.pop()

                    for raw_line in lines:
                        line = raw_line.decode("utf-8").strip()
                        if not line:
                            continue

                        chunk.append(line)
                        if len(chunk) == self._chunk_size * self._gcs_record_size:
                            await queue.put(chunk)
                            chunk = []

                if buffer:
                    line = buffer.decode("utf-8").strip()
                    if line:
                        chunk.append(line)

            if chunk:
                await queue.put(chunk)
        except (asyncssh.SFTPError, UnicodeDecodeError):
            logger.exception("Error reading file %s", remote_path)
            raise
        finally:
            await queue.put(None)

    async def _consumer(
        self,
        queue: asyncio.Queue,
        day_folder: GSPath,
        filename: str,
        publish_time: datetime,
    ) -> None:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break

            logger.info("Received chunk of %d lines from %s", len(chunk), filename)

            records = [
                build_record(
                    lines=chunk[i:i + self._gcs_record_size],
                    publish_time=publish_time,
                    attributes={
                        "protocol": "SFTP",
                        "source_host": self._sftp_host,
                        "source_name": self._source_name,
                        # "time": publish_time.isoformat(),
                    },
                )
                for i in range(0, len(chunk), self._gcs_record_size)
            ]

            avro_bytes = serialize_chunk(records)

            out_path = day_folder / output_filename(publish_time)
            await self._gcs_client.upload(
                out_path.bucket,
                out_path.blob,
                avro_bytes,
                content_type="application/octet-stream",
            )
            logger.info("Written %s", out_path)

    def _generate_filenames(self, datetime_from: datetime, datetime_to: datetime) -> list[str]:
        filenames = []
        current = datetime_from
        while current < datetime_to:
            filenames.append(current.strftime(self._sftp_filename_format))
            current += timedelta(minutes=5)

        return filenames

    def _gcs_day_folder(self, base_path: GSPath, filename: str) -> GSPath:
        file_dt = datetime.strptime(filename, self._sftp_filename_format)
        return base_path / file_dt.strftime('%Y-%m-%d')

    async def _write_success_file(self, day_folder: GSPath, filename: str) -> None:
        success_path = day_folder / f"{SUCCESS_FILE_PREFIX}{filename}"
        await self._gcs_client.upload(
            success_path.bucket,
            success_path.blob,
            b"",
            content_type="text/plain",
        )
        logger.info("Written success file %s", success_path)
