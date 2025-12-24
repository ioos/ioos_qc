# IOOS QC

[![conda_forge_package](https://anaconda.org/conda-forge/ioos_qc/badges/version.svg)](https://anaconda.org/conda-forge/ioos_qc)
[![Default-Tests](https://github.com/ioos/ioos_qc/actions/workflows/tests.yml/badge.svg)](https://github.com/ioos/ioos_qc/actions/workflows/tests.yml)

Collection of utilities, scripts and tests to assist in automated
quality assurance and quality control for oceanographic datasets and
observing systems.

[Code](https://github.com/ioos/ioos_qc) \|
[Issues](https://github.com/ioos/ioos_qc/issues) \|
[Documentation](https://ioos.github.io/ioos_qc/)

## ERDDAP URLs (CSV / NetCDF)

ioos_qc can be run against **public ERDDAP dataset URLs** by first turning the URL
into an existing Stream type (pandas for CSV, xarray for NetCDF). Once loaded,
QC behavior matches local-file workflows.

- **Supported URL formats** (best-effort detection):
  - `.../tabledap/<dataset>.csv?...` (also `.csvp`, `.csv0`)
  - `.../tabledap/<dataset>.nc?...` (also `.ncCF`, `.nc4`, `.cdf`)

Use `ioos_qc.streams.stream_from_path_or_erddap_url(...)` to create a Stream and
then run QC the normal way with `ioos_qc.config.Config`.
