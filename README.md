# DuckLake + Vortex (Firebolt fork)

This is Firebolt's fork of [DuckLake](https://github.com/duckdb/ducklake) that adds support for
[Vortex](https://vortex.dev) as a data-file format alongside Parquet. A DuckLake table can store its
data files as Parquet (the default) or as Vortex, and a single table may contain a mix of both — the
scanner dispatches per file based on the `file_format` recorded in `ducklake_data_file`.

> This fork tracks upstream DuckLake but is pinned to an older base so that DuckLake and the Vortex
> DuckDB extension build against the same DuckDB ABI. See [Versioning & building](#versioning--building).

## Using Vortex

Set the format per catalog / schema / table via the `data_file_format` option (default `parquet`):

```sql
ATTACH 'ducklake:my.db' AS lake (DATA_PATH 'data', METADATA_CATALOG 'meta');

-- write new data files for this table as vortex
CALL lake.set_option('data_file_format', 'vortex');
-- vortex tables must not inline data (see limitations below)
CALL lake.set_option('data_inlining_row_limit', 0);

CREATE TABLE lake.t(i INTEGER, s VARCHAR, l INTEGER[]);
INSERT INTO lake.t VALUES (1, 'a', [1, 2]), (2, NULL, []);
SELECT * FROM lake.t WHERE i >= 2;          -- reads via read_vortex, with filter pushdown
```

Switching the option back to `parquet` and inserting again produces Parquet files in the same table;
both formats are read transparently.

## What works

### Read

| Capability | Status |
| --- | --- |
| Scan `file_format = 'vortex'` data files (via `read_vortex`) | ✅ |
| Mixed Parquet + Vortex files in one table (per-file dispatch) | ✅ |
| Projection pushdown | ✅ |
| Filter pushdown (files without column stats are never wrongly pruned) | ✅ |
| Aggregation, `COUNT(*)` | ✅ |
| Positional deletes / deletion vectors over Vortex files | ✅ |
| Column rename (physical names resolved via the file's name map) | ✅ |
| Time-travel over mixed-format snapshots | ✅ |

### Write

| Capability | Status |
| --- | --- |
| `INSERT` into a Vortex table | ✅ |
| `CREATE TABLE AS` in Vortex format | ✅ |
| Multiple inserts (each produces a Vortex file) | ✅ |
| `DELETE` / `UPDATE` on Vortex-backed rows | ✅ |
| Types: INTEGER, VARCHAR, DOUBLE, BOOLEAN, `LIST`, `STRUCT`, NULLs, nested | ✅ |
| `NOT NULL` enforcement (streaming validation, aborts on NULL) | ✅ |

## What does not work yet

These are rejected with a clear `Not implemented` error rather than silently misbehaving. They all stem
from the same root cause: **Vortex files carry no per-file column statistics**, which several DuckLake
operations rely on (null counts, min/max, embedded snapshot/row-id ranges), or from Vortex's
single-file write model.

| Capability | Behavior | Why |
| --- | --- | --- |
| Partitioned Vortex tables | rejected | needs one file per partition value + recorded `partition_values` |
| Flushing inlined data (`data_inlining_row_limit > 0`) | rejected | recovers `begin_snapshot` / `row_id_start` from written snapshot/row-id **stats** |
| Compaction (`ducklake_merge_adjacent_files`) | rejected | its directory+rotation output does not compose with the single-file write path yet |
| Encryption of Vortex files | rejected | only Parquet writes support the encryption config |
| `NOT NULL` on `CREATE TABLE AS` | not enforced | the constraint is not threaded through the CTAS copy input (enforced for `INSERT`) |
| Writing `json` / `csv` as data files | rejected | those use DuckDB's plan-based COPY, not the `copy_to_bind` API DuckLake drives |

**Practical guidance:** for Vortex tables, keep `data_inlining_row_limit = 0` and avoid partitioning.
A write-heavy Vortex table will accumulate many small files that currently cannot be compacted.

## Versioning & building

Vortex's DuckDB extension targets DuckDB **stable releases** (e.g. v1.5.x), while DuckLake `main`
tracks DuckDB **`main`**. The two diverge (notably DuckDB moved `Vector`'s data/validity into
`VectorBuffer` on `main`), so they cannot build against the same DuckDB. This fork is therefore based
on the newest DuckLake commit that still predates that refactor, with the `duckdb` submodule pinned to
the commit the Vortex extension targets.

Vortex is **opt-in** at build time (it compiles a Rust crate via Corrosion and needs a Rust toolchain):

```sh
ENABLE_VORTEX=1 GEN=ninja make release      # builds ducklake + the ./vortex submodule
```

Without `ENABLE_VORTEX`, the build is plain DuckLake (this is what the standard distribution CI builds,
against the stable DuckDB release — so **CI binaries do not include Vortex**). A prebuilt Vortex-enabled
linux-amd64 shell is available under [`amd64/`](amd64/).

The Vortex reader/writer only needs the `vortex` extension's `read_vortex` table function and
`COPY ... (FORMAT vortex)`; it drives them through DuckLake's multi-file reader and copy paths.
