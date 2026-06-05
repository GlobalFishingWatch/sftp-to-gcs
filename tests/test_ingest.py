import pytest

from sftp_to_gcs.ingest import load_sftp_password


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
