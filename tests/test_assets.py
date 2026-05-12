import json
from importlib import resources

from sftp_to_gcs import assets


def test_data():
    with open(resources.files(assets) / "data.json") as file:
        data = json.load(file)

    assert data == {"value": 1234}
