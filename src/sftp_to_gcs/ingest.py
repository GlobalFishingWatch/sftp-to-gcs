import logging
from typing import Optional, Any

from cloudpathlib import GSPath

logger = logging.getLogger(__name__)


def run(
    project: str,
    sftp_host: str,
    sftp_user: str,
    sftp_directory: str,
    sftp_pass_env: str,
    datetime_from: str,
    datetime_to: str,
    gcs_path: str,
    chunk_size: int = 12_500,
    concurrency: int = 20,
    unknown_unparsed_args: Optional[list[str]] = None,
    unknown_parsed_args: Optional[dict[str, Any]] = None,
) -> None:

    gcs_path = GSPath(gcs_path)
