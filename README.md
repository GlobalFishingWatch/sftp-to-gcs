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

A lightweight CLI tool that transfers files from an SFTP server to Google Cloud Storage.

**Features**:
* :white_check_mark: Transfer files from SFTP to GCS with configurable chunk size.
* :white_check_mark: CLI interface for easy integration with orchestration tools.
* :white_check_mark: Suitable for scheduled or event-driven batch workflows.


[docker compose]: https://docs.docker.com/compose/install/linux/

[cli.py]: src/sftp_to_gcs/cli.py
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

TBC.

### Minimum Requirements

TBC.


### Repository Overview

_This section applies only to the template and provides an overview of the repository contents._

<div  align="justify">

### Using the CLI

_Write instructions on how to use the CLI of the application here._

#### Config file example

_**Optional**_.
_Provide an example of an input configuration file._

## Data persistence pattern

_**Optional**_.
_Explain the data persistence pattern used in this application._

## How to Contribute

Please read the guidelines in [CONTRIBUTING.md].

### Git Workflow

Please refer to our [git workflow documentation] to know how to manage branches in this repository.

## Implementation details

_**Optional**_.
_This section is for describing implementation details, primarily for developers._

### Most relevant modules

_**Optional**_.
_Use this section to describe the most important modules of your application._

Example:
<div align="center">

| Module | Description |
| --- | --- |
| [cli.py]     | Defines the application CLI. |

</div>