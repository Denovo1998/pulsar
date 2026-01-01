# Tiered Storage / OpenDAL offloader

This module provides a Pulsar tiered storage (ledger offload) implementation based on the
Apache OpenDAL Java binding.

The build produces the offloader NAR:

- `tiered-storage-opendal-<version>.nar` (Maven artifact: `org.apache.pulsar:tiered-storage-opendal`)

## Supported driver names (no broker config change)

The OpenDAL offloader is compatible with the existing driver names so that existing broker
configuration continues to work:

- `aws-s3` / `S3`
- `aliyun-oss`
- `google-cloud-storage`
- `azureblob`
- `transient` (tests/debug only)

## Distribution packaging

The default offloader distribution is expected to include only one “cloud” offloader NAR to
avoid discovery conflicts (multiple NARs claiming the same driver names).

## Migration / rollback (operational guidance)

These steps only affect what NAR is present under `${PULSAR_HOME}/offloaders`:

- Migration (jcloud → opendal):
  - Stop the broker.
  - Remove any `tiered-storage-jcloud-*.nar` from `${PULSAR_HOME}/offloaders`.
  - Ensure `tiered-storage-opendal-*.nar` is present in `${PULSAR_HOME}/offloaders`.
  - Start the broker. Existing `managedLedgerOffloadDriver=aws-s3|...` settings stay the same.

- Rollback (opendal → jcloud):
  - Stop the broker.
  - Remove `tiered-storage-opendal-*.nar` from `${PULSAR_HOME}/offloaders`.
  - Restore `tiered-storage-jcloud-*.nar` into `${PULSAR_HOME}/offloaders`.
  - Start the broker.

## Compatibility constraints (must not change)

To keep historical offloaded data readable across implementations, the OpenDAL offloader is
required to preserve these invariants:

- Object key naming stays the same as the legacy JCloud implementation:
  - `uuid-ledger-<id>`, `uuid-ledger-<id>-index`, `uuid-index`, etc.
- Data object binary format is unchanged:
  - `DataBlockHeader(128B) + entries + padding`
  - Padding semantics must remain compatible with the existing “negative `readInt()` triggers seek fixup” behavior.
- Index object binary format is unchanged:
  - Index block V1/V2: `magic word + length + metadata + sparse index entries`
  - Serialization rules must match the existing implementation.
- Format version field stays unchanged:
  - `S3ManagedLedgerOffloaderFormatVersion=1`
  - Metadata key normalization differs across backends, so reads must tolerate key case differences and/or missing keys
    with a safe fallback (without breaking reads of existing data).
