import pytest
import aiohttp
from unittest.mock import AsyncMock, MagicMock

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


def mock_sftp_factory(lines: list[str] = None):
    lines = lines or []

    encoded = b"\n".join(line.encode() for line in lines) + b"\n"
    reads = [encoded, b""]

    async def mock_read(size):
        return reads.pop(0) if reads else b""

    mock_file = AsyncMock()
    mock_file.__aenter__ = AsyncMock(return_value=mock_file)
    mock_file.__aexit__ = AsyncMock(return_value=None)
    mock_file.read = mock_read

    mock_sftp = AsyncMock()
    mock_sftp.open = MagicMock(return_value=mock_file)
    mock_sftp.__aenter__ = AsyncMock(return_value=mock_sftp)
    mock_sftp.__aexit__ = AsyncMock(return_value=None)

    mock_conn = AsyncMock()
    mock_conn.start_sftp_client = MagicMock(return_value=mock_sftp)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)

    return lambda *args, **kwargs: mock_conn


def test_run_does_nothing_when_all_files_already_complete(monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "secret")
    args = [
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--sftp-filename-format", "ais-%Y-%m-%d-%H-%M.nmea",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T00:10",
        "--source-name", "kpler",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--buffer-size", "250000",
        "--concurrency", "20",
    ]
    main.run(args, storage_factory=mock_storage_factory(exists=True))


def test_run_processes_files_when_none_complete(monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "secret")
    args = [
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--sftp-filename-format", "ais-%Y-%m-%d-%H-%M.nmea",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T00:10",
        "--source-name", "kpler",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--buffer-size", "250000",
        "--concurrency", "20",
    ]
    main.run(
        args,
        storage_factory=mock_storage_factory(exists=False),
        sftp_factory=mock_sftp_factory(
            lines=[
                r"\c:1778524200,s:ter*57\!BSVDM,1,1,0,B,13m;PL`0000dWnfQhl83>hWn0D0P,0*3A",
                r"\c:1778524200,s:ter*57\!BSVDM,1,1,0,B,13m;PL`0000dWnfQhl83>hWn0D0P,0*3A",
            ]
        ),
    )


def test_run_reads_password_from_file(tmp_path):
    secret = tmp_path / "sftp-password"
    secret.write_text("s3cr3t")
    args = [
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-path", str(secret),
        "--sftp-directory", "/data",
        "--sftp-filename-format", "ais-%Y-%m-%d-%H-%M.nmea",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T00:10",
        "--source-name", "kpler",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--buffer-size", "250000",
        "--concurrency", "20",
    ]
    main.run(args, storage_factory=mock_storage_factory(exists=True))


def test_run_raises_value_error_when_sftp_pass_not_set(monkeypatch):
    monkeypatch.delenv("SFTP_PASS", raising=False)
    args = [
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--sftp-filename-format", "ais-%Y-%m-%d-%H-%M.nmea",
        "--datetime-from", "2026-04-21T00:00",
        "--datetime-to", "2026-04-21T00:10",
        "--source-name", "kpler",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--buffer-size", "250000",
        "--concurrency", "20",
    ]
    with pytest.raises(ValueError, match="Environment variable 'SFTP_PASS' not found"):
        main.run(args)
