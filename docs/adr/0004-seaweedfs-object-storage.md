# ADR 0004: SeaweedFS instead of MinIO for S3-compatible storage

Status: accepted (2026-10)

## Context

The stack needs local S3-compatible object storage. MinIO was the obvious choice, but as of 2026-10 its community server is archived: `dl.min.io` answers `410 Gone` ("no longer maintained ... no security updates") and no maintained community images exist.

## Decision

SeaweedFS 4.48 (Apache-2.0, actively released), single-process `weed server -s3`.

## Consequences

* Works with Iceberg `S3FileIO` (AWS SDK v2) and Trino's native S3 file system, with path-style access.
* Newer AWS SDKs add CRC checksums by default; both JVMs run with `aws.requestChecksumCalculation=WHEN_REQUIRED` for portability across S3-compatible stores.
* Production would point the same settings at Amazon S3 or another object store; nothing else changes.
