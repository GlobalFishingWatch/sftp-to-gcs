# avro.py
import io
from datetime import datetime
from uuid import uuid4

import fastavro

AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "ingestion_message",
    "fields": [
        {
            "name": "message_id",
            "type": "string"
        },
        {
            "name": "publish_time",
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        },
        {
            "name": "attributes",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "data",
            "type": "bytes"
        },
    ],
})


def build_record(
    lines: list[str],
    publish_time: datetime,
    attributes: dict[str, str] = None,
) -> dict:
    """Builds an AVRO record from a group of NMEA sentences.

    Args:
        lines:
            A group of NMEA sentences to include in the record's data field.

        publish_time:
            Timestamp used as ingestion time for all sentences in the group.

        attributes:
            Metadata attributes to include in the record.

    Returns:
        A dictionary matching the Message AVRO schema.
    """
    attributes = attributes or {}
    return {
        "message_id": uuid4().hex,
        "publish_time": publish_time,
        "attributes": attributes,
        "data": "\r\n".join(lines).encode("utf-8"),
    }


def serialize_chunk(records: list[dict]) -> bytes:
    """Serializes a list of AVRO records to bytes.

    Args:
        records:
            List of dictionaries matching the Message AVRO schema.

    Returns:
        AVRO-serialized bytes.
    """
    buffer = io.BytesIO()
    fastavro.writer(buffer, AVRO_SCHEMA, records)
    buffer.seek(0)
    return buffer.read()


def output_filename(publish_time: datetime) -> str:
    """Generates an output AVRO filename.

    Args:
        publish_time:
            Publish time of the ingested message.

    Returns:
        Filename in the format ``{HH_MM_SSZ}_{short_uuid}.avro``.
    """
    return f"{publish_time.strftime('%H_%M_%SZ')}_{uuid4().hex[:6]}.avro"
