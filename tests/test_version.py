from sftp_to_gcs import version


def test_version():
    assert isinstance(version.__version__, str)
