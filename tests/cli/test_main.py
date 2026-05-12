import pytest
import aiohttp
from unittest.mock import AsyncMock

from sftp_to_gcs.cli import main


def mock_storage_factory(exists: bool = True):
    mock = AsyncMock()
    if exists:
        mock.download_metadata = AsyncMock(return_value={})
    else:
        error = aiohttp.ClientResponseError(request_info=None, history=None, status=404)
        mock.download_metadata = AsyncMock(side_effect=error)
    mock.__aenter__ = AsyncMock(return_value=mock)
    mock.__aexit__ = AsyncMock(return_value=None)
    return lambda **kwargs: mock


def test_run_does_nothing_when_all_files_already_complete(monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "secret")
    args = [
        "--project", "my-project",
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T01:00",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--chunk-size", "12500",
        "--concurrency", "20",
    ]
    main.run(args, storage_factory=mock_storage_factory(exists=True))


def test_run_processes_files_when_none_complete(monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "secret")
    args = [
        "--project", "my-project",
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T01:00",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--chunk-size", "12500",
        "--concurrency", "20",
    ]
    with pytest.raises(NotImplementedError):
        main.run(args, storage_factory=mock_storage_factory(exists=False))


def test_run_raises_value_error_when_sftp_pass_not_set(monkeypatch):
    monkeypatch.delenv("SFTP_PASS", raising=False)
    args = [
        "--project", "my-project",
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T01:00",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--chunk-size", "12500",
        "--concurrency", "20",
    ]
    with pytest.raises(ValueError, match="'SFTP_PASS' environment variable is not set"):
        main.run(args)
