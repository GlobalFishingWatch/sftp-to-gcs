import asyncio

import aiohttp
import pytest

from cloudpathlib import GSPath

from sftp_to_gcs.ingest import SftpToGcsIngester, load_sftp_password


def test_reads_password_from_file(tmp_path):
    secret = tmp_path / "sftp-password"
    secret.write_text("s3cr3t")
    assert load_sftp_password(str(secret), None) == "s3cr3t"


def test_strips_trailing_newline_from_file(tmp_path):
    secret = tmp_path / "sftp-password"
    secret.write_text("s3cr3t\n")
    assert load_sftp_password(str(secret), None) == "s3cr3t"


def test_raises_when_file_is_empty(tmp_path):
    secret = tmp_path / "sftp-password"
    secret.write_text("")
    with pytest.raises(ValueError, match="exists but is empty"):
        load_sftp_password(str(secret), None)


def test_falls_back_to_env_when_file_path_not_provided(monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "s3cr3t")
    assert load_sftp_password(None, "SFTP_PASS") == "s3cr3t"


def test_falls_back_to_env_when_file_does_not_exist(tmp_path, monkeypatch):
    monkeypatch.setenv("SFTP_PASS", "s3cr3t")
    missing = str(tmp_path / "sftp-password")
    assert load_sftp_password(missing, "SFTP_PASS") == "s3cr3t"


def test_raises_when_env_var_not_set(monkeypatch):
    monkeypatch.delenv("SFTP_PASS", raising=False)
    with pytest.raises(ValueError, match="Environment variable 'SFTP_PASS' not found"):
        load_sftp_password(None, "SFTP_PASS")


def test_raises_when_neither_source_provided():
    with pytest.raises(ValueError, match="No sFTP password source provided"):
        load_sftp_password(None, None)


def test_file_takes_precedence_over_env(tmp_path, monkeypatch):
    secret = tmp_path / "sftp-password"
    secret.write_text("from-file")
    monkeypatch.setenv("SFTP_PASS", "from-env")
    assert load_sftp_password(str(secret), "SFTP_PASS") == "from-file"


class FakeGCSClient:
    """Records peak in-flight download_metadata calls and fails on demand."""

    def __init__(self, errors: list[Exception] | None = None):
        self.errors = list(errors or [])
        self.calls = 0
        self.in_flight = 0
        self.max_in_flight = 0

    async def download_metadata(self, bucket, blob):
        self.calls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0.01)
            if self.errors:
                raise self.errors.pop(0)
            return {}
        finally:
            self.in_flight -= 1


def _response_error(status: int) -> aiohttp.ClientResponseError:
    return aiohttp.ClientResponseError(request_info=None, history=(), status=status)


def _ingester(concurrency: int = 20) -> SftpToGcsIngester:
    return SftpToGcsIngester(
        sftp_host="host",
        sftp_port=22,
        sftp_user="user",
        sftp_pass="pass",
        sftp_directory="/dir",
        sftp_filename_format="ais-%Y-%m-%d-%H-%M.nmea",
        source_name="test",
        gcs_path=GSPath("gs://bucket/path"),
        concurrency=concurrency,
    )


DAY_FOLDER = GSPath("gs://bucket/path/2026-09-18")


def test_success_file_check_respects_concurrency():
    ingester = _ingester(concurrency=5)
    client = FakeGCSClient()

    async def check_all():
        return await asyncio.gather(*[
            ingester._success_file_exists(client, DAY_FOLDER, f"file-{i}")
            for i in range(100)
        ])

    results = asyncio.run(check_all())

    assert all(results)
    assert client.calls == 100
    assert client.max_in_flight == 5


def test_success_file_check_returns_false_on_404():
    client = FakeGCSClient(errors=[_response_error(404)])
    assert asyncio.run(_ingester()._success_file_exists(client, DAY_FOLDER, "f")) is False


def test_success_file_check_retries_on_timeout():
    client = FakeGCSClient(errors=[asyncio.TimeoutError()])
    assert asyncio.run(_ingester()._success_file_exists(client, DAY_FOLDER, "f")) is True
    assert client.calls == 2


def test_success_file_check_raises_on_non_transient_error():
    client = FakeGCSClient(errors=[_response_error(403)])
    with pytest.raises(aiohttp.ClientResponseError):
        asyncio.run(_ingester()._success_file_exists(client, DAY_FOLDER, "f"))
    assert client.calls == 1
