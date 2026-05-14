<h1 align="center" style="border-bottom: none;">  sftp-to-gcs </h1>

<p align="center">
  <a href="https://github.com/GlobalFishingWatch/sftp-to-gcs/actions/workflows/main.yaml" >
    <img src="https://github.com/GlobalFishingWatch/sftp-to-gcs/actions/workflows/main.yaml/badge.svg"/>
  </a>
  <a href="https://codecov.io/gh/GlobalFishingWatch/sftp-to-gcs" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/sftp-to-gcs/graph/badge.svg?token=uZTb6EphP8"/>
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.13%20%7C%203.14-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/sftp-to-gcs">
  </a>
</p>

A lightweight and dockerized CLI tool that transfers files from an SFTP server to Google Cloud Storage.

**Features**:
* :white_check_mark: Transfer files from SFTP to GCS with configurable chunk size.
* :white_check_mark: CLI interface for easy integration with orchestration tools.
* :white_check_mark: Suitable for scheduled or event-driven batch workflows.


[docker compose]: https://docs.docker.com/compose/install/linux/

[avro.py]: src/sftp_to_gcs/avro.py
[cli/main.py]: src/sftp_to_gcs/cli/main.py
[ingest.py]: src/sftp_to_gcs/ingest.py

[CONTRIBUTING.md]: CONTRIBUTING.md
[examples]: examples/
[git workflow documentation]: docs/contributing/GITHUB-FLOW.md
[src/sftp_to_gcs/]: src/sftp_to_gcs 
[src/sftp_to_gcs/assets/]: src/sftp_to_gcs/assets/



## Introduction

<div align="justify">

Originally built as a fallback mechanism for AIS data ingestion: when a provider's TCP
stream becomes unavailable, this tool picks up the gap by fetching files over SFTP and
delivering them to GCS for downstream batch processing.

## Usage

### Using the CLI

The component is configured via CLI arguments or a YAML config file.

```shell
(.venv) [tom@tlink sftp-to-gcs]$ sftp-to-gcs -h
usage: sftp-to-gcs (v0.1.0). [-h] [-c ] [-v] [--log-file ] [--log-to-stdout] [--no-rich-logging] [--only-render] [--sftp-host ] [--sftp-port ] [--sftp-user ] [--sftp-pass-env ]
                             [--sftp-directory ] [--sftp-filename-format ] [--datetime-from ] [--datetime-to ] [--gcs-path ] [--gcs-record-size ] [--chunk-size ] [--concurrency ]

A CLI tool that transfers files from an SFTP server to Google Cloud Storage.

options:
  -h, --help               show this help message and exit

built-in CLI options:
  -c, --config-file        Path to config file. (default: None)
  -v, --verbose            Set logger level to DEBUG. (default: False)
  --log-file               File to send logging output to. (default: None)
  --log-to-stdout          If True, sends logs output to sys.stdout stream. (default: False)
  --no-rich-logging        Disable rich logging [useful for production environments]. (default: False)
  --only-render            Dry run, only renders command line call and prints it. (default: False)

options defined by 'sftp-to-gcs' command:
  --sftp-host              SFTP server hostname. [required] (default: None)
  --sftp-port              SFTP server port. (default: 22)
  --sftp-user              SFTP username. [required] (default: None)
  --sftp-pass-env          Name of the environment variable containing the SFTP password. [required] (default: None)
  --sftp-directory         Remote SFTP directory to read files from. [required] (default: None)
  --sftp-filename-format   SFTP filename format (e.g. ais-%Y-%m-%d-%H-%M.nmea). [required] (default: None)
  --datetime-from          Start datetime of the range to process (inclusive), format YYYY-MM-DDTHH:MM. [required] (default: None)
  --datetime-to            End datetime of the range to process (exclusive), format YYYY-MM-DDTHH:MM. [required] (default: None)
  --gcs-path               Destination GCS path where AVRO files will be written (e.g. gs://bucket/dir/). [required] (default: None)
  --gcs-record-size        Number of sentences per AVRO record. (default: 20)
  --chunk-size             Number of lines per output AVRO file. (default: 12500)
  --concurrency            Maximum number of SFTP files processed concurrently. (default: 20)

Examples:
    sftp_to_gcs -h
    sftp_to_gcs -c config/example.yaml
```

### Running

The SFTP password is read from the environment variable specified by `sftp_pass_env`:

```bash
export SFTP_PASS=your-password
```

```bash
sftp-to-gcs -c config.yaml
```

#### Config file example

```yaml
sftp_host: s-6f8262c824f94eeea.server.transfer.eu-west-1.amazonaws.com
sftp_user: global-fishing-watch-nmea
sftp_pass_env: KPLER_SFTP_PASSWORD
sftp_directory: /global-fishing-watch-nmea
sftp_filename_format: "ais-%Y-%m-%d-%H-%M.nmea"
datetime_from: "2026-05-11T20:00"
datetime_to: "2026-05-11T20:10"
gcs_path: gs://scratch-tomas-ttl30d/kpler-sftp/
gcs_record_size: 20
chunk_size: 12500
concurrency: 20
```

## How to Contribute

Please read the guidelines in [CONTRIBUTING.md].

### Git Workflow

Please refer to our [git workflow documentation] to know how to manage branches in this repository.

## Implementation details

_**Optional**_.
_This section is for describing implementation details, primarily for developers._

TBC.

### Most relevant modules

<div align="center">

| Module | Description |
| --- | --- |
| [cli/main.py] | Defines the application CLI.                                          |
| [ingest.py]   | SFTP to GCS ingestion orchestration, streaming and chunking logic.    |
| [avro.py]     | AVRO schema definition and serialization utilities for NMEA messages. |

</div>