# Apache Arrow 23 Parquet writer — feature parity (delivered state)

## Status

Delivered. This records the outcome of bringing Velox's vendored Parquet writer
(`velox/dwio/parquet/writer/arrow/`, forked from Arrow 15.0.0) up to **Arrow
23.0.1 feature parity**, on top of the Arrow 23 build/upgrade branch. It
supersedes the incremental plan in `parquet-writer-arrow23-resync.md` for the
feature-porting phase and documents what was ported, what was intentionally
scoped out, and why.

Everything below builds into the full `velox` library and is validated by the
vendored writer suite: **578 tests pass**, `libvelox.a` links.

## Approach

The writer stays a **fork** (FBThrift, `-DARROW_PARQUET=OFF`); parity was reached
by porting the *semantically meaningful* Arrow 15→23 changes onto the fork,
translated to Velox conventions (camelCase, FBThrift, glog, memory pool), not by
re-vendoring wholesale. New Arrow files are vendored and rehomed only where a
feature needs them.

## Delivered

### New logical types (Arrow 23)
- `parquet.thrift`: `Float16Type`, `VariantType`, `GeometryType`,
  `GeographyType` structs + `EdgeInterpolationAlgorithm` enum, wired into the
  `LogicalType` union (ids 15–18). FBThrift regenerates.
- `Types.{h,cpp}`: `Float16LogicalType`, `VariantLogicalType`,
  `GeometryLogicalType`, `GeographyLogicalType` — factories, `is*` predicates,
  `fromThrift` dispatch (FBThrift field-refs), `Impl` classes with
  `toString`/`toJson`/`toThrift`/`equals`, and the edge-algorithm ↔ thrift
  converters. Round-trip test included.
- `Types.cpp` minor deltas: `formatFloat16Value` + a float16 branch in
  `formatStatValue`; deprecated `ConvertedType` TIME/TIMESTAMP → `LogicalType`
  conversions with named-argument parity.

### Geospatial statistics (end to end)
- Vendored `GeospatialStatistics`, `GeospatialUtilInternal` (WKB bounding box),
  `GeospatialUtilJsonInternal` (CRS metadata, RapidJSON), rehomed.
- `parquet.thrift`: `BoundingBox` + `GeospatialStatistics` structs +
  `ColumnMetaData.geospatial_statistics` (id 17).
- `ThriftInternal.h`: `EncodedGeoStatistics` ↔ thrift converters (FBThrift).
- `Metadata`: `ColumnChunkMetaDataBuilder::setGeospatialStatistics`.
- `ColumnWriter`: per-chunk `GeoStatistics` accumulator for geometry columns,
  updated with WKB values on the packed and spaced write paths, encoded into
  column-chunk metadata on close.

### Content-defined chunking (CDC)
- Vendored `ContentDefinedChunker` (rolling gearhash + generated gear table),
  rehomed (common `LevelInfo`, glog).
- `Properties`: `CdcOptions` + enable/disable/options builder methods +
  accessors, threaded through `WriterProperties`.
- `ColumnWriter`: `writeArrow` splits input at content-defined boundaries and
  flushes a data page at each (last chunk excepted, so a subsequent
  `writeArrow` continues the page); `writeBatch`/`writeBatchSpaced` reject CDC.
  Off by default. Property test included.

### Correctness fixes (found during the residual-delta review)
- **INT32_MAX page-size guards**: `writeDictionaryPage`/`writeDataPage` throw
  when the uncompressed or compressed page size exceeds INT32_MAX, instead of
  silently truncating the int32 page-header size field into corrupt Parquet.
- **Non-nullable column validation**: `writeArrow` returns `Status::Invalid`
  when a leaf declared non-nullable contains nulls.

### Size statistics
Column-chunk and page-level histograms + unencoded bytes — landed earlier on the
Arrow 23 upgrade branch; present and on by default.

## Build-infrastructure solutions

Two non-obvious blockers were solved and are load-bearing:

- **RapidJSON** (needed by geospatial): resolved as a BUNDLED FetchContent
  dependency (`CMake/resolve_dependency_modules/rapidjson.cmake`, pinned to
  Arrow 23's commit) and **consumed via an include directory**, *not* a linked
  target — linking a bundled IMPORTED target into the exported mono `velox`
  library breaks the CMake generate/install step.
- **C++20 modules**: Velox builds the mono library with `-fmodules-ts`. The
  vendored Arrow-derived sources (which open namespaces and include `Types.h`)
  poison the header-unit graph, importing `Types.h` inside the writer namespace
  and breaking every writer file. Fix: `CXX_SCAN_FOR_MODULES OFF` on the
  vendored geospatial/chunker sources.

## Scoped out (intentional, with reasons)

- **`max_rows_per_page`** — deferred. The straightforward implementation (a
  row-count flush check) corrupts data (10 tests failed, verified); the correct
  upstream design requires restructuring the hot `DoInBatches` write loop with
  row-capping. Niche option, high regression risk on the core path. Reverted.
- **Encryption `SecureString` modernization** — not done, by decision. It is a
  ~2375-line atomic refactor (keys → `::arrow::util::SecureString`, `span` APIs,
  `AesCryptoContext`) across all callers. It is **functionally equivalent** (the
  fork already encrypts/decrypts correctly and passes tests) and **internally
  unused** — Velox does not drive Parquet encryption from its own path. High
  risk (silent crypto corruption), lowest utility. Left as a documented pickup.
- **ArrowSchema arrow-extension mapping** (`geoarrow.wkb` /
  `VariantExtensionType` arrays → logical types) — **blocked**: those Arrow
  extension types are not installed in this Arrow build, and the path is not
  used by Velox's writer (Velox drives the writer from its own type system, so
  the logical types are reachable without it).

## Reader-side performance optimizations (orthogonal)

User perf work on `iemejia/parquet-perf-backup` is **reader-side** (decoders,
`PageReader`, decompression) and does not touch the vendored writer, so it is
orthogonal to this parity effort. Note: two of those optimizations
(BYTE_STREAM_SPLIT decoder, PDEP shift+mask fallback) are **already upstream**;
the remaining ~6 (constant-delta miniblock skip, batch boolean decode, decoder
`reset()` reuse, direct LZ4, O(1) fixed-length string skip, redundant Snappy
length skip) are **unique** and would be integrated by a **selective**
cherry-pick, skipping the two upstreamed ones (a naive replay conflicts).

## Reusable tooling

`scripts/vendor/` — `parquet-arrow-manifest.txt` (canonical upstream→vendored
map), `parquet-arrow-vendor.sh` (verbatim import), `parquet-arrow-rehome.py`
(deterministic namespace/include/license rehoming). These make the next Arrow
bump reproducible.

## Validation

Full `velox` library links; the vendored writer suite runs **578 tests, all
passing**, including the new logical-type round-trip and CDC property tests, with
no regression to the pre-existing 576.
