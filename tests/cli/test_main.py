from sftp_to_gcs.cli import main


def test_run_calls_ingest_with_correct_arguments():
    args = [
        "--project", "my-project",
        "--sftp-host", "sftp.example.com",
        "--sftp-user", "user",
        "--sftp-pass-env", "SFTP_PASS",
        "--sftp-directory", "/data",
        "--datetime-from", "2026-04-21",
        "--datetime-to", "2026-04-28",
        "--gcs-path", "gs://my-bucket/nmea-ftp-backfill/",
        "--chunk-size", "12500",
        "--concurrency", "20",
    ]

    main.run(args)
