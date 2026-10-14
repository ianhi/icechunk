# Querying Many Repositories

Every Icechunk repository keeps its metadata in a few kinds of file: a snapshot file lists every group and array, manifests record where each chunk is stored, and a repo info file lists branches, tags and commits.
Reading these files gives a repository's metadata without walking its hierarchy group by group.

The files are encoded with [FlatBuffers](https://flatbuffers.dev), a compact binary format that Icechunk reads quickly but text tools cannot.
`icechunk inspect` converts them to JSON, so you can query them with standard tools: jq to filter, and SQL in [DuckDB](https://duckdb.org) for questions that span many rows.
It reads any number of repositories, local or in S3, in one command.
A query can use two kinds of metadata:

- **Zarr metadata:** each group's and array's `zarr.json`, with its path, shape, data type, chunk shape, codecs, fill value, dimension names and attributes. Icechunk stores these in the snapshot file.
- **Icechunk metadata:** for every chunk, whether it is a stored object, inline in the manifest, or a byte range of another file, with its size and location, from the manifests. For the commit, its id, message, time and metadata, from the repo info file.

Chunk objects and the files behind virtual chunks are never read, with [one exception](#32-small-values-from-inline-chunks) for very small arrays.
It does not require any specific metadata schema from the repositories: a query can use whatever fields they contain.

The example collection is six repositories from one group, each holding a different kind of data: micrographs, an accelerometer log, gene counts, a survey table, simulation output and detector frames.
They do not share a data format, but the group tags each root group with attributes, among them `project`, which is `atlas` or `outreach`; `status`, which is `active` or `archived`; and `license`.
Each import also recorded, in its commit's metadata, whether it checked the license.
The repositories sit on local disk as subdirectories of `datasets/`, such as `datasets/micrographs`, so `datasets/*` reads all six.

Before a data release, the group needs every active `atlas` repository with an array of more than a million elements: how much data it stores, under which license, and whether that license was checked at import.
The answer draws on all three kinds of row: array shapes and root attributes from `nodes`, chunk sizes from `chunks`, and commit metadata from `commits`.
The three commands print into one stream, and a DuckDB query, saved as `release.sql`, combines the rows of each repository:

```console
$ { icechunk inspect nodes datasets/*; icechunk inspect chunks datasets/*; \
    icechunk inspect commits datasets/*; } | duckdb -table -f release.sql
+---------------+---------------+-----------+-----------+---------------+
|     name      | largest_array | stored_mb |  license  | license_check |
+---------------+---------------+-----------+-----------+---------------+
| micrographs   | 1310720       | 1.8       | CC-BY-4.0 | spdx          |
| vibration-log | 1200000       | 4.4       | cc-by 4.0 | none          |
+---------------+---------------+-----------+-----------+---------------+
```

Two repositories qualify, and they spell the same license differently.
The commit metadata shows why: the import of `vibration-log` did not check its license.
The guide builds this query step by step, then fixes the license on a branch.

`icechunk inspect` has two kinds of command:

- **Row commands** (`nodes`, `chunks`, `commits`) print one JSON line per group, array, chunk reference or repository, across many repositories at once. You search with these.
- **File commands** (`repo`, `snapshot`, `manifest`, `txlog`) print one format file as a single JSON document, for looking at the format itself.

A search reads one version of each repository, `main` unless you pass `--ref`. Earlier commits are not searched.

!!! tip "A second example"

    [Example: Searching DANDI Recordings](querying-dandi.md) applies the same commands to real neuroscience recordings whose chunks are virtual references into a public S3 bucket.

!!! warning "Prototype"

    The `icechunk inspect` command exists only on the `flatbuffer-queries` branch. It reads local and `s3://` repositories, and has been tried only on small collections like the ones in these guides.

The guide has four parts:

1. **One repository's metadata** (sections 1–3): the collection, then each kind of row: Zarr metadata from `nodes`, and chunk references and commits from `chunks` and `commits`.
2. **Querying many repositories** (sections 4–6): searching without knowing the keys, SQL across rows, and one query over all three kinds of row.
3. **Acting on the answer** (sections 7–8): opening the data, and fixing a value on a branch.
4. **Beyond queries** (sections 9–10): browsing one repository as files, and what the commands cannot see.

A [reference](#reference) of row fields and options closes the page.

## Before you start

You need the `icechunk` command-line tool, [jq](https://jqlang.org), DuckDB, and [uv](https://docs.astral.sh/uv/) to run the Python scripts.

**The CLI.** Build it from the `flatbuffer-queries` branch of the Icechunk repository and put it on your `PATH`. The build needs Rust 1.94.1 or newer.

```bash
git checkout flatbuffer-queries
cargo build -p icechunk --features cli --bin icechunk
export PATH="$PWD/target/debug:$PATH"
```

**DuckDB.** Any recent DuckDB command-line tool works. This runs one through uv without installing it:

```bash
alias duckdb="uvx --from duckdb-cli duckdb"
```

The commands in this guide pass `-table`, which draws result tables with plain ASCII characters.

**jq** filters JSON on the command line. This guide uses a few pieces of it:

- `select(condition)` keeps the rows where the condition holds.
- `{a, b}` keeps only some fields, and `del(.a)` drops one.
- `"\(.a)  \(.b)"` formats fields into a line of text.
- `-c` prints each result on one line, and `-r` prints strings without quotes.

## 1. The example collection

The collection is six repositories, each holding one synthetic dataset of a different kind, so the guide does not depend on one scientific field. A script writes them in a few seconds, without network access. Save it as `make_datasets.py` and run it in an empty directory:

<details markdown="1">
<summary><code>make_datasets.py</code></summary>

```python
# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = ["icechunk>=2.3", "zarr>=3.1", "numpy>=2"]
# ///
"""Write a collection of datasets of different kinds, one Icechunk repository each."""
import sys
from pathlib import Path

import icechunk
import numpy as np
import zarr

TEXT = np.dtypes.StringDType()


def micrographs(root, rng):
    z, y, x = np.mgrid[0:20, 0:256, 0:256]
    img = 1000 + 400 * np.sin(x / 20 + z / 5) * np.cos(y / 30)
    img += rng.normal(0, 20, img.shape)
    root.create_array(
        "images", data=img.astype("uint16"), chunks=(1, 256, 256),
        dimension_names=["z", "y", "x"],
    )
    root.create_array("pixel_size", data=np.float64(2.5),
                      attributes={"units": "nm"})


def vibration_log(root, rng):
    t = np.arange(400_000) / 2000
    phase = rng.uniform(0, 2 * np.pi, (3, 1))
    accel = 0.5 * np.sin(2 * np.pi * 50 * t + phase)
    accel += rng.normal(0, 0.05, accel.shape)
    root.create_array(
        "acceleration", data=accel.astype("float32"), chunks=(3, 50_000),
        dimension_names=["axis", "time"], attributes={"units": "m s-2"},
    )
    root.create_array("axis_names",
                      data=np.array(["x", "y", "z"], dtype=TEXT))
    root.create_array("sampling_rate", data=np.float64(2000),
                      attributes={"units": "Hz"})


def gene_counts(root, rng):
    counts = rng.poisson(0.3, (2000, 48)).astype("int32")
    root.create_array("counts", data=counts, chunks=(500, 48),
                      dimension_names=["gene", "sample"])
    root.create_array("genes", data=np.array(
        [f"gene_{i:04d}" for i in range(2000)], dtype=TEXT))
    root.create_array("samples", data=np.array(
        [f"s{i:02d}" for i in range(48)], dtype=TEXT))


def survey_table(root, rng):
    n = 1200
    table = root.create_group("responses")
    table.create_array("id", data=np.arange(n, dtype="int32"))
    table.create_array("age", data=rng.integers(18, 90, n).astype("int16"))
    table.create_array("score", data=rng.uniform(0, 10, n).astype("float32"))
    regions = np.array(["north", "south", "east", "west"], dtype=TEXT)
    table.create_array("region", data=regions[rng.integers(0, 4, n)])


def sim_output(root, rng):
    field = rng.normal(0, 1, (20, 32, 32)).cumsum(axis=0).round(2)
    root.create_array(
        "pressure", data=field.astype("float32"), chunks=(1, 8, 8),
        dimension_names=["step", "y", "x"], attributes={"units": "Pa"},
    )


def legacy_scans(root, rng):
    scans = rng.integers(0, 4096, (5, 512, 512)).astype("uint16")
    root.create_array("scans", data=scans, chunks=(1, 512, 512),
                      dimension_names=["scan", "y", "x"])


# name: (builder, description, root attributes, commit metadata)
DATASETS = {
    "micrographs": (
        micrographs, "Electron micrographs of an aluminium alloy, 20 slices",
        {"owner": "mlopez", "project": "atlas", "license": "CC-BY-4.0",
         "status": "active", "tags": ["imaging"], "created": "2025-03-14"},
        {"source": "microscope export", "pipeline_version": "2.3",
         "license_check": "spdx"},
    ),
    "vibration-log": (
        vibration_log, "Three-axis accelerometer on a pump housing",
        {"owner": "kchen", "project": "atlas", "license": "cc-by 4.0",
         "status": "active", "tags": ["timeseries"], "created": "2025-05-02"},
        {"source": "logger upload", "pipeline_version": "0.9",
         "license_check": "none"},
    ),
    "gene-counts": (
        gene_counts, "Read counts for 2000 genes in 48 samples",
        {"owner": "mlopez", "project": "atlas", "license": "CC-BY-4.0",
         "status": "active", "tags": ["omics"], "created": "2025-06-20"},
        {"source": "sequencing core", "pipeline_version": "2.3",
         "license_check": "spdx"},
    ),
    "survey-table": (
        survey_table, "Questionnaire responses, one row per person",
        {"owner": "apatel", "project": "outreach", "license": "CC0-1.0",
         "status": "active", "tags": ["table"], "created": "2025-07-09"},
        {"source": "form export", "pipeline_version": "2.3",
         "license_check": "spdx"},
    ),
    "sim-output": (
        sim_output, "Pressure field from 20 solver steps",
        {"owner": "kchen", "project": "atlas", "license": "CC-BY-4.0",
         "status": "active", "tags": ["simulation"], "created": "2025-08-30"},
        {"source": "solver run 118", "pipeline_version": "2.3",
         "license_check": "spdx"},
    ),
    "legacy-scans": (
        legacy_scans, "Detector frames migrated from the old archive",
        {"owner": "apatel", "project": "atlas",
         "status": "archived", "tags": ["imaging"], "created": "2019-11-01"},
        {"source": "archive migration", "pipeline_version": "1.4",
         "license_check": "none"},
    ),
}

out = Path(sys.argv[1])
for seed, (name, (build, description, attrs, meta)) in enumerate(
    DATASETS.items()
):
    storage = icechunk.local_filesystem_storage(str(out / name))
    session = icechunk.Repository.create(storage).writable_session("main")
    root = zarr.create_group(session.store, attributes=attrs)
    root.create_array("description", data=np.array(description, dtype=TEXT))
    build(root, np.random.default_rng(seed))
    print(name, session.commit(f"Import {name}", metadata=meta))
```

</details>

```console
$ uv run make_datasets.py datasets
micrographs VXXPFMVH0P7JWRB69PT0
vibration-log YCWDG9PA2JP96R2DDX5G
gene-counts J3F2KBCWPDYQ12WEJ2QG
survey-table 2NBZMBWBHBDSCP3HRMKG
sim-output JH3AHZ54MCRWFF1EM25G
legacy-scans P62JA7SZY0JHVXDAAZW0
```

The script prints each repository and the snapshot id of its one commit. Snapshot ids are random, so yours will differ from the ones in this guide.

- Each script in this guide starts with a comment block listing its dependencies, which `uv run` reads and installs.
- Icechunk 2.3.0 also logs warnings that local filesystem storage is unsafe for concurrent commits. They do not affect this example.

Each repository is an ordinary Icechunk repository. What they hold differs:

```text
datasets/
├── micrographs    images: uint16 [20, 256, 256], an image stack
├── vibration-log  acceleration: float32 [3, 400000], three axes
├── gene-counts    counts: int32 [2000, 48], with row and column labels
├── survey-table   responses/: a group of four 1-D columns of 1200 rows
├── sim-output     pressure: float32 [20, 32, 32], 20 solver steps
└── legacy-scans   scans: uint16 [5, 512, 512], detector frames
```

The group's import script tags each root group with the attributes `owner`, `project`, `license`, `status`, `tags` and `created`, and writes a `description` array holding one string. Nothing in Icechunk requires these; they are what this group happened to agree on. Here is `datasets/vibration-log` in full:

```text
/                  group: owner, project, license, status, tags, created
├── description    string scalar
├── axis_names     string [3]
├── sampling_rate  float64 scalar, units Hz
└── acceleration   float32 [3, 400000], units m s-2, chunks of [3, 50000]
```

The commit of each import records the `source` of the data, the `pipeline_version` that imported it, and whether the import checked the license.
The repositories are not perfectly consistent, and the queries find where they differ.

## 2. Zarr metadata: one row per node

A node is a group or an array. `icechunk inspect nodes` prints one row for every node in a repository, holding that node's Zarr metadata: the contents of its `zarr.json`, with its path and the repository and snapshot it came from. It reads only the repository's snapshot. Here is its whole output for the smallest repository, `legacy-scans`:

```console
$ icechunk inspect nodes datasets/legacy-scans
{"attributes":{"created":"2019-11-01","owner":"apatel","project":"atlas","status":"archived","tags":["imaging"]},"kind":"group","path":"/","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0","zarr":{"node_type":"group","zarr_format":3}}
{"attributes":{},"chunk_grid":[],"dimension_names":null,"kind":"array","path":"/description","repo":"datasets/legacy-scans","shape":[],"snapshot":"P62JA7SZY0JHVXDAAZW0","zarr":{"chunk_grid":{"configuration":{"chunk_shape":[]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{},"name":"vlen-utf8"},{"configuration":{"checksum":false,"level":0},"name":"zstd"}],"data_type":"string","fill_value":"","node_type":"array","shape":[],"storage_transformers":[],"zarr_format":3}}
{"attributes":{},"chunk_grid":[5,1,1],"dimension_names":["scan","y","x"],"kind":"array","path":"/scans","repo":"datasets/legacy-scans","shape":[5,512,512],"snapshot":"P62JA7SZY0JHVXDAAZW0","zarr":{"chunk_grid":{"configuration":{"chunk_shape":[1,512,512]},"name":"regular"},"chunk_key_encoding":{"configuration":{"separator":"/"},"name":"default"},"codecs":[{"configuration":{"endian":"little"},"name":"bytes"},{"configuration":{"checksum":false,"level":0},"name":"zstd"}],"data_type":"uint16","dimension_names":["scan","y","x"],"fill_value":0,"node_type":"array","shape":[5,512,512],"storage_transformers":[],"zarr_format":3}}
```

Each line is one JSON object: the root group, the `description` array and the `scans` array. The commands in the rest of this guide pipe these lines through jq or DuckDB to pick out rows and fields. Here is the row for the acceleration array in `vibration-log`, pretty-printed by jq and without its `zarr` field:

```console
$ icechunk inspect nodes datasets/vibration-log \
    | jq 'select(.path == "/acceleration") | del(.zarr)'
{
  "attributes": {
    "units": "m s-2"
  },
  "chunk_grid": [
    1,
    8
  ],
  "dimension_names": [
    "axis",
    "time"
  ],
  "kind": "array",
  "path": "/acceleration",
  "repo": "datasets/vibration-log",
  "shape": [
    3,
    400000
  ],
  "snapshot": "YCWDG9PA2JP96R2DDX5G"
}
```

Every row says which repository and which snapshot it came from. `shape` and `chunk_grid` give the array's size and its number of chunks along each dimension. `attributes` holds the node's Zarr attributes, and `zarr` holds the rest of its `zarr.json`, such as `data_type`, `codecs` and `fill_value`.

The row for `/description` records its shape and attributes but not the text:

```console
$ icechunk inspect nodes datasets/vibration-log \
    | jq -c 'select(.path == "/description") | {path, shape, attributes}'
{"path":"/description","shape":[],"attributes":{}}
```

The text is the contents of the array's one chunk, and the snapshot does not hold chunk contents.

## 3. Icechunk metadata: chunks and commits

### 3.1 Chunk references

A manifest records each chunk reference in one of three ways:

- **Native:** the chunk is an object stored in the repository.
- **Inline:** the chunk's bytes are stored in the manifest itself. Icechunk inlines chunks up to 512 bytes by default, so a short string like the description is inline.
- **Virtual:** the chunk is a byte range of a file outside the repository. The [DANDI example](querying-dandi.md) is built from virtual chunks.

`icechunk inspect chunks` prints one row per chunk reference, read from the manifests. Here is its whole output for `legacy-scans`, whose five detector frames are one chunk each, plus the inline chunk of `description`:

```console
$ icechunk inspect chunks datasets/legacy-scans
{"coord":[],"kind":"inline","length":62,"path":"/description","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
{"chunk_id":"D9XC5SNS2QBVR9BZK0M0","coord":[0,0,0],"kind":"native","length":448559,"offset":0,"path":"/scans","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
{"chunk_id":"YPB7KY7H88YKBYATJGA0","coord":[1,0,0],"kind":"native","length":448407,"offset":0,"path":"/scans","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
{"chunk_id":"MNV15XWXX7CATTSYWYCG","coord":[2,0,0],"kind":"native","length":448497,"offset":0,"path":"/scans","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
{"chunk_id":"CVW2BA6RFS1J3C75EQM0","coord":[3,0,0],"kind":"native","length":448475,"offset":0,"path":"/scans","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
{"chunk_id":"P26WDEM79P284SRMGT8G","coord":[4,0,0],"kind":"native","length":448409,"offset":0,"path":"/scans","repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
```

Pretty-printed by jq, here is the first chunk of the image stack in `micrographs`:

```console
$ icechunk inspect chunks datasets/micrographs \
    | jq 'select(.path == "/images" and .coord == [0, 0, 0])'
{
  "chunk_id": "EABS0H2M8T6WP86X3YKG",
  "coord": [
    0,
    0,
    0
  ],
  "kind": "native",
  "length": 91203,
  "offset": 0,
  "path": "/images",
  "repo": "datasets/micrographs",
  "snapshot": "VXXPFMVH0P7JWRB69PT0"
}
```

A native chunk names the object that holds it (`chunk_id`) and its size in bytes. A virtual chunk names a `location` and `offset` in another file instead.

### 3.2 Small values from inline chunks

The text of `description` is in its inline chunk, so it can be read without fetching a chunk object. `--decode-inline` on `nodes` does that. For every array with one chunk and at most 64 elements, it reads the array's manifest and, if the chunk is inline, decodes it and adds a `value` field to the row. `has("value")` keeps the rows that got one:

```console
$ icechunk inspect nodes datasets/vibration-log --decode-inline \
    | jq -c 'select(has("value")) | {path, value}'
{"path":"/axis_names","value":["x","y","z"]}
{"path":"/description","value":"Three-axis accelerometer on a pump housing"}
{"path":"/sampling_rate","value":2000.0}
```

An array whose chunk is native or virtual does not get a `value`, because decoding it would mean fetching a chunk object. `--decode-inline` never does.

The flag is off by default because it reads one manifest per small array, where `nodes` alone reads only the snapshot. It decodes the codec pipelines zarr-python writes for small strings and numbers, `vlen-utf8` or `bytes`, each optionally followed by `zstd`, and leaves other pipelines undecoded.

### 3.3 Commits

`icechunk inspect commits` prints one row per repository for the commit its branch points to, read from the repo info file:

```console
$ icechunk inspect commits datasets/legacy-scans
{"flushed_at":"2026-10-14T15:00:29.914770Z","message":"Import legacy-scans","metadata":{"license_check":"none","pipeline_version":"1.4","source":"archive migration"},"repo":"datasets/legacy-scans","snapshot":"P62JA7SZY0JHVXDAAZW0"}
```

The `metadata` field holds whatever the commit's author wrote. Here it is the import's `source`, `pipeline_version` and `license_check`.

## 4. Many repositories at once

The row commands take any number of repositories and print all of their rows in one stream. Rows arrive in the order the repositories finish reading, so the examples below `sort` them.

Each command reads only the files it needs:

| File | What it holds | How many are read per repository |
|---|---|---|
| Repo info | Branches, tags, and each commit's message and metadata | One |
| Snapshot | Every group and array in one version: its path, its shape, and its whole `zarr.json`, attributes included | One for `nodes` and `chunks`; not read by `commits` |
| Manifest | The references to an array's chunks | Not read by `nodes`; one per small array for `nodes --decode-inline`; every array's, or those under `--path`, for `chunks` |

These repositories are on local disk, where reading them takes milliseconds. In object storage the cost is request latency.
For `nodes`, each repository needs a few requests in sequence: the repo info file, then the snapshot.
The command reads 16 repositories at a time, so their latencies overlap; `--concurrency` changes that number.
Reading the snapshots of eight public ERA5 repositories in S3 from a laptop took one to two seconds.

### 4.1 What the collection holds

You don't need to know a collection's attributes before searching it. List every attribute key in all six repositories, with how many nodes use it:

```console
$ icechunk inspect nodes datasets/* \
    | jq -r '.attributes | keys[]' | sort | uniq -c | sort -rn
   6 tags
   6 status
   6 project
   6 owner
   6 created
   5 license
   4 units
```

Six root groups carry the import script's attributes, but only five have a `license`. Four arrays have `units`, which nobody coordinated: each dataset's author added them where they made sense.

### 4.2 Searching any field

Any key the listing turned up can be searched. These are the arrays with units, in whichever repository they are:

```console
$ icechunk inspect nodes datasets/* \
    | jq -r 'select(.attributes.units)
             | "\(.repo)  \(.path)  \(.attributes.units)"' \
    | sort
datasets/micrographs  /pixel_size  nm
datasets/sim-output  /pressure  Pa
datasets/vibration-log  /acceleration  m s-2
datasets/vibration-log  /sampling_rate  Hz
```

A search doesn't even need a key. `tostring` turns the whole row into text, so `test()` matches a word anywhere in it: a path, an attribute, a dimension name, or, with `--decode-inline`, a small array's contents. It also matches the `repo` field and the rest of `zarr.json`, so a repository's name matches every row of that repository. To search attributes only, use `select(.attributes | tostring | test(...))`.

```console
$ icechunk inspect nodes datasets/* --decode-inline \
    | jq -r 'select(tostring | test("pump"; "i")) | "\(.repo)  \(.path)"'
datasets/vibration-log  /description
```

The word appears only in the text of a `description` array, decoded from its inline chunk.

### 4.3 One attribute across every repository

Ask every repository for its license:

```console
$ icechunk inspect nodes datasets/* \
    | jq -r 'select(.path == "/") | "\(.repo)  \(.attributes.license)"' \
    | sort
datasets/gene-counts  CC-BY-4.0
datasets/legacy-scans  null
datasets/micrographs  CC-BY-4.0
datasets/sim-output  CC-BY-4.0
datasets/survey-table  CC0-1.0
datasets/vibration-log  cc-by 4.0
```

The licenses are not consistent. `vibration-log` spells CC-BY-4.0 as `cc-by 4.0`, and `legacy-scans` does not have a `license` attribute, which is the missing one from section 4.1, so jq prints `null`.

jq looks at one row at a time. That was enough for "the license of each repository", because the license lives in the root group's row. The release question also needs the arrays: a repository qualifies when its root row says `atlas` and `active` *and* one of its array rows has more than a million elements. That condition spans several rows of the same repository. jq can collect them with `jq -s 'group_by(.repo)'`, but the condition is much shorter in SQL.

## 5. Questions across rows: SQL

[DuckDB](https://duckdb.org) runs SQL directly on JSON rows. `read_json` turns each JSON line into a table row and each field into a column. A field that holds a JSON object, such as `attributes`, becomes a struct, so `attributes.license` reads one key. A key that a row does not have reads as `NULL`.

### 5.1 One row per repository

Every repository has exactly one root group, so its root row can stand for the repository. Save this as `datasets.sql`:

```sql
SELECT parse_filename(repo) AS name,
       attributes.project, attributes.status, attributes.license
FROM read_json('/dev/stdin')
WHERE path = '/'
ORDER BY name;
```

- `read_json('/dev/stdin')` reads the rows from the pipe, so they are not written to disk.
- `WHERE path = '/'` keeps the root group's row of each repository.
- `parse_filename(repo)` is a built-in DuckDB function that returns the last part of a path, like Python's `os.path.basename`. `repo` holds the repository as given on the command line, such as `datasets/micrographs`, so this gives its name.

```console
$ icechunk inspect nodes datasets/* \
    | duckdb -table -f datasets.sql
+---------------+----------+----------+-----------+
|     name      | project  |  status  |  license  |
+---------------+----------+----------+-----------+
| gene-counts   | atlas    | active   | CC-BY-4.0 |
| legacy-scans  | atlas    | archived | NULL      |
| micrographs   | atlas    | active   | CC-BY-4.0 |
| sim-output    | atlas    | active   | CC-BY-4.0 |
| survey-table  | outreach | active   | CC0-1.0   |
| vibration-log | atlas    | active   | cc-by 4.0 |
+---------------+----------+----------+-----------+
```

### 5.2 Keeping some repositories

Conditions on the root attributes go in the `WHERE` clause. Save this as `active_atlas.sql`, which adds the highlighted line:

```sql hl_lines="4"
SELECT parse_filename(repo) AS name,
       attributes.project, attributes.status, attributes.license
FROM read_json('/dev/stdin')
WHERE path = '/' AND attributes.project = 'atlas' AND attributes.status = 'active'
ORDER BY name;
```

```console
$ icechunk inspect nodes datasets/* \
    | duckdb -table -f active_atlas.sql
+---------------+---------+--------+-----------+
|     name      | project | status |  license  |
+---------------+---------+--------+-----------+
| gene-counts   | atlas   | active | CC-BY-4.0 |
| micrographs   | atlas   | active | CC-BY-4.0 |
| sim-output    | atlas   | active | CC-BY-4.0 |
| vibration-log | atlas   | active | cc-by 4.0 |
+---------------+---------+--------+-----------+
```

### 5.3 A condition on other rows: large arrays

The size condition is about array rows, not the root row. The query computes the largest array of each repository, then joins that to the root rows.

A pipe can be read only once, but a join reads its input twice. `WITH rows AS MATERIALIZED (...)` reads the pipe once into a table named `rows`, and the rest of the query reads from that table. Each further name in the `WITH` clause is a step of the query. Save this as `large_atlas.sql`:

```sql
WITH rows AS MATERIALIZED (FROM read_json('/dev/stdin')),
repos AS (
    SELECT parse_filename(repo) AS name, attributes.project, attributes.status,
           attributes.license
    FROM rows WHERE path = '/'
),
arrays AS (
    SELECT parse_filename(repo) AS name,
           max(list_product(shape))::BIGINT AS largest_array
    FROM rows WHERE kind = 'array' GROUP BY name
)
SELECT name, largest_array, license
FROM repos JOIN arrays USING (name)
WHERE project = 'atlas' AND status = 'active' AND largest_array > 1000000
ORDER BY name;
```

- `repos` is the query from section 5.1: one row per repository.
- `arrays` has one row per repository too. `list_product(shape)` multiplies an array's shape into its number of elements, and `GROUP BY name` with `max()` keeps each repository's largest.
- `JOIN ... USING (name)` puts the two side by side, matching rows by repository name.

```console
$ icechunk inspect nodes datasets/* \
    | duckdb -table -f large_atlas.sql
+---------------+---------------+-----------+
|     name      | largest_array |  license  |
+---------------+---------------+-----------+
| micrographs   | 1310720       | CC-BY-4.0 |
| vibration-log | 1200000       | cc-by 4.0 |
+---------------+---------------+-----------+
```

`gene-counts` and `sim-output` drop out: their largest arrays have 96,000 and 20,480 elements. Every value in these tables came from the snapshots; the command did not read any chunk.

### 5.4 Chunk rows in SQL

Chunk rows work the same way. Save this as `storage.sql`, which summarizes how each repository stores its chunks. `FILTER (kind = 'inline')` makes an aggregate count only the rows that match:

```sql
SELECT parse_filename(repo)                  AS name,
       count(*)                              AS chunks,
       count(*) FILTER (kind = 'inline')     AS inline,
       sum(length) FILTER (kind = 'native')  AS native_bytes,
       sum(length) FILTER (kind = 'inline')  AS inline_bytes
FROM read_json('/dev/stdin')
GROUP BY name
ORDER BY name;
```

```console
$ icechunk inspect chunks datasets/* \
    | duckdb -table -f storage.sql
+---------------+--------+--------+--------------+--------------+
|     name      | chunks | inline | native_bytes | inline_bytes |
+---------------+--------+--------+--------------+--------------+
| gene-counts   | 7      | 2      | 31807        | 176          |
| legacy-scans  | 6      | 1      | 2242347      | 62           |
| micrographs   | 22     | 2      | 1840705      | 87           |
| sim-output    | 321    | 321    | NULL         | 83098        |
| survey-table  | 5      | 1      | 9278         | 60           |
| vibration-log | 11     | 3      | 4386343      | 104          |
+---------------+--------+--------+--------------+--------------+
```

Most repositories store each large array in a few native chunks: 20 chunks of about 90 KB for the image stack, 8 of about 550 KB for the acceleration, and 5 of about 450 KB for the scans. Their small arrays are inline.

`sim-output` is different. Its pressure field was written in chunks of 8 × 8 values, so its 320 chunks are each under 512 bytes and all inline, and it does not have native chunks (`NULL`). All of its data lives in its manifest, and reading the field means decoding 320 chunks.

## 6. Combining kinds of metadata

### 6.1 Why the licenses differ

The two repositories with a bad license might share a cause. Each import recorded its pipeline version and whether it checked the license against the [SPDX license list](https://spdx.org/licenses/), and `commits` reads that for every repository:

```console
$ icechunk inspect commits datasets/* \
    | jq -r '.metadata as $m
             | "\(.repo)  \($m.pipeline_version)  \($m.license_check)"' \
    | sort
datasets/gene-counts  2.3  spdx
datasets/legacy-scans  1.4  none
datasets/micrographs  2.3  spdx
datasets/sim-output  2.3  spdx
datasets/survey-table  2.3  spdx
datasets/vibration-log  0.9  none
```

`.metadata as $m` names the metadata object, to keep the line short. The two repositories with a bad license, `vibration-log` and `legacy-scans`, are the two whose import did not check the license, by pipelines 0.9 and 1.4. Every repository imported by 2.3 has a valid SPDX identifier.

### 6.2 One query over all three kinds of row

The release question needs Zarr metadata, chunk sizes and commit metadata together. A shell group, `{ ...; }`, runs the three commands one after another into a single pipe, and `read_json` reads their rows as one table. A column that a kind of row does not have is `NULL` in that row: chunk rows have no `shape`, and only commit rows have a `message`.

The query extends `large_atlas.sql` with two more steps, one per new kind of row. Save this as `release.sql`:

```sql hl_lines="12-21"
WITH rows AS MATERIALIZED (FROM read_json('/dev/stdin')),
repos AS (
    SELECT parse_filename(repo) AS name, attributes.project, attributes.status,
           attributes.license
    FROM rows WHERE path = '/'
),
arrays AS (
    SELECT parse_filename(repo) AS name,
           max(list_product(shape))::BIGINT AS largest_array
    FROM rows WHERE kind = 'array' GROUP BY name
),
stored AS (
    SELECT parse_filename(repo) AS name, round(sum(length) / 1e6, 1) AS stored_mb
    FROM rows WHERE kind = 'native' GROUP BY name
),
commits AS (
    SELECT parse_filename(repo) AS name, metadata.license_check
    FROM rows WHERE message IS NOT NULL
)
SELECT name, largest_array, stored_mb, license, license_check
FROM repos JOIN arrays USING (name) JOIN stored USING (name) JOIN commits USING (name)
WHERE project = 'atlas' AND status = 'active' AND largest_array > 1000000
ORDER BY name;
```

- `stored` adds up the sizes of each repository's native chunks. Node rows also have a `kind`, `group` or `array`, so `kind = 'native'` leaves them out.
- `commits` keeps the commit rows, the only rows with a `message`, and reads one key of their `metadata`.

```console
$ { icechunk inspect nodes datasets/*; icechunk inspect chunks datasets/*; \
    icechunk inspect commits datasets/*; } | duckdb -table -f release.sql
+---------------+---------------+-----------+-----------+---------------+
|     name      | largest_array | stored_mb |  license  | license_check |
+---------------+---------------+-----------+-----------+---------------+
| micrographs   | 1310720       | 1.8       | CC-BY-4.0 | spdx          |
| vibration-log | 1200000       | 4.4       | cc-by 4.0 | none          |
+---------------+---------------+-----------+-----------+---------------+
```

The `nodes` rows supply the shapes and the license, the `chunks` rows the stored size, and the `commits` row the license check.

## 7. Opening what you found

The query gives repository names, and the rows give array paths, which is all you need to open the data with Zarr. Save this as `open_dataset.py`:

```python
# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = ["icechunk>=2.3", "zarr>=3.1"]
# ///
import icechunk
import zarr

storage = icechunk.local_filesystem_storage("datasets/vibration-log")
repo = icechunk.Repository.open(storage)
root = zarr.open_group(repo.readonly_session("main").store, mode="r")
accel = root["acceleration"]
print(accel.shape, accel.attrs["units"])
print(accel[:, :3])
```

```console
$ uv run open_dataset.py
(3, 400000) m s-2
[[-0.10226245 -0.06938154 -0.1670523 ]
 [-0.17675321 -0.10115629  0.04444609]
 [ 0.33776587  0.5258854   0.38630596]]
```

## 8. Fixing a value on a branch

Fix `vibration-log` on a new branch, so that `main` stays as it was. Save this as `fix_license.py`:

```python
# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = ["icechunk>=2.3", "zarr>=3.1"]
# ///
"""Correct a repository's license spelling on a new branch."""
import sys

import icechunk
import zarr

storage = icechunk.local_filesystem_storage(sys.argv[1])
repo = icechunk.Repository.open(storage)
repo.create_branch("fix-license", repo.lookup_branch("main"))
session = repo.writable_session("fix-license")
root = zarr.open_group(session.store, mode="r+")
root.attrs["license"] = "CC-BY-4.0"
print(session.commit(
    "Normalize license spelling",
    metadata={"source": "license cleanup", "license_check": "spdx"},
))
```

```console
$ uv run fix_license.py datasets/vibration-log
SVK0TK28XP7D3M24WXA0
```

Every row command takes `--ref`: a branch, tag or snapshot id to read in every repository it is given. The default is `main`. A repository that lacks the ref is reported on stderr, the others still print, and the command exits with an error:

```console
$ icechunk inspect commits datasets/micrographs datasets/vibration-log \
    --ref fix-license | jq -r '"\(.repo)  \(.message)"'
datasets/micrographs: `fix-license` is not a valid snapshot id, branch, or tag
Error: ❌ 1 repository could not be read
datasets/vibration-log  Normalize license spelling
```

Read `vibration-log` at both versions. `--arg ref $ref` passes the shell variable into jq as `$ref`:

```console
$ for ref in main fix-license; do
    icechunk inspect nodes datasets/vibration-log --ref $ref \
      | jq -r --arg ref $ref 'select(.path == "/")
               | "\($ref)  \(.snapshot)  \(.attributes.license)"'
  done
main  YCWDG9PA2JP96R2DDX5G  cc-by 4.0
fix-license  SVK0TK28XP7D3M24WXA0  CC-BY-4.0
```

On `fix-license`, `vibration-log` has the license CC-BY-4.0, and `main` is unchanged. Each row names the snapshot it came from, so keeping that id with a query result pins the result to exact contents, even after the branch moves on.

## 9. Browsing one repository by hand

### 9.1 Exporting a repository as files

To browse a repository rather than query it, `icechunk inspect export` writes one repository out as text files: a directory for every group and array, mirroring the hierarchy, each holding the node's `zarr.json`, a `chunks.ndjson` with one line per chunk reference, and a `value.json` for each array `--decode-inline` would decode. It reports what it wrote on stderr, which `2> /dev/null` hides here.

```console
$ icechunk inspect export datasets/vibration-log json/vibration-log 2> /dev/null
$ ls -1 json/vibration-log/axis_names
chunks.ndjson
value.json
zarr.json
$ cat json/vibration-log/axis_names/value.json
[
  "x",
  "y",
  "z"
]
```

The top of the export also holds `snapshot.json`, the same row `icechunk inspect commits` prints, so the export records which version it came from.

Ordinary tools work on the export. Export every repository, and `grep -L` lists the root groups that lack a license:

```console
$ for r in datasets/*; do
    icechunk inspect export $r json/${r#*/} 2> /dev/null
  done
$ grep -L '"license"' json/*/zarr.json
json/legacy-scans/zarr.json
```

### 9.2 The format files as documents

To see the format files themselves, the file commands of `icechunk inspect` print one file as a JSON document, the same output as Python's `Repository.inspect_*` methods:

| Command | Prints |
|---|---|
| `icechunk inspect repo REPO` | Branches, tags and the list of snapshots |
| `icechunk inspect snapshot REPO [--ref REF]` | Node ids, paths, shapes, and the list of manifests |
| `icechunk inspect manifest REPO MANIFEST_ID` | Chunk counts by kind for one manifest |
| `icechunk inspect txlog REPO [--ref REF]` | What the commit changed |

In these repositories each array has its own manifest; repositories written with other settings can share one manifest between arrays. The manifest holding `sim-output`'s 320 inline chunks is the large one:

```console
$ icechunk inspect snapshot datasets/sim-output | jq -c '.manifests[]'
{"id":"5BGNM04DBVV7Z2JA2HD0","size_bytes":78053,"num_chunk_refs":320}
{"id":"PMHDADPRYWTCF44G6T6G","size_bytes":183,"num_chunk_refs":1}
```

## 10. What the commands cannot see

**Small arrays that are virtual.** `--decode-inline` decodes only inline chunks. A scalar whose chunk is a virtual reference into another file does not get a `value`. If you build repositories you want to query, store small metadata arrays in the repository rather than as virtual references, so their chunks are small enough to be inline, or copy the values into attributes.

**Other codecs.** `--decode-inline` decodes `vlen-utf8` and `bytes`, optionally followed by `zstd`. An inline chunk written with any other codec pipeline does not get a `value`.

**Many small manifests.** When each array has its own manifest, `--decode-inline` reads one manifest per small array. In object storage that is one request per small array, 16 at a time within each repository.

**Columns that appear late.** `read_json` infers its columns from a sample of the first rows. In a large stream, a field that only later rows have, such as the commit rows' `message` after millions of chunk rows, can be left out, and the query fails with an error that the column is not found. `read_json('/dev/stdin', sample_size = -1)` infers the columns from every row instead.

**Snapshots in time.** Rows describe the repositories when the command ran. Rows saved to a file (`icechunk inspect nodes ... > nodes.ndjson`) go stale when the repositories change; the `snapshot` field in each row records which version it came from.

## Reference

### Rows

`icechunk inspect nodes` prints one row per group or array:

| Field | Contents |
|---|---|
| `repo` | The repository as given on the command line |
| `snapshot` | The snapshot id that `--ref` resolved to |
| `path` | The node's path, such as `/acceleration` |
| `kind` | `group` or `array` |
| `shape` | Array length per dimension (arrays only) |
| `chunk_grid` | Number of chunks per dimension (arrays only) |
| <code style="white-space: nowrap">dimension_names</code> | Dimension names, or `null` (arrays only) |
| `attributes` | The node's Zarr attributes |
| `zarr` | The rest of the node's `zarr.json` |
| `value` | With `--decode-inline`: the decoded contents of a single-chunk array of at most 64 elements whose chunk is inline, or its fill value if the chunk was never written. Decoded codec pipelines: `vlen-utf8` or `bytes`, each optionally followed by `zstd` |

`icechunk inspect chunks` prints one row per chunk reference: `repo`, `snapshot`, `path`, `coord`, `kind` (`virtual`, `native` or `inline`), and `length`. A virtual chunk adds `location` and `offset`; a native chunk adds `chunk_id` and `offset`.

`icechunk inspect commits` prints one row per repository: `repo`, `snapshot`, `flushed_at`, `message` and `metadata`.

Rows from several repositories arrive in the order the repositories finish reading, not sorted.

### Options

`nodes`, `chunks` and `commits` accept:

| Option | Meaning |
|---|---|
| `REPOS...` | Repositories as config aliases, local paths or `s3://bucket/prefix` URLs. `s3://bucket/prefix/*` expands to every repository directly under the prefix |
| `--repos-from FILE` | Read more repositories from a file, one per line, or from stdin with `-` |
| `--ref REF` | Branch, tag or snapshot id to read in every repository (default `main`) |
| `--anonymous` | Read `s3://` repositories without credentials |
| `--region REGION` | Region of `s3://` buckets; defaults to `AWS_REGION` or `AWS_DEFAULT_REGION` |
| `--concurrency N` | Repositories read at the same time (default 16) |
| `--decode-inline` | `nodes` only: add a `value` to small arrays whose chunk is inline; reads one manifest per such array |
| `--path PREFIX` | `chunks` only: read only the manifests of arrays at or under this path |

A repository that cannot be opened is reported on stderr. The others still print, and the command exits with an error.

`icechunk inspect export REPO DIR` takes one repository, plus `--ref`, `--anonymous`, `--region` and `--path`.
