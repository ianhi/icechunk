# Example: Searching DANDI Recordings

[DANDI](https://dandiarchive.org) is a public archive of neurophysiology data, organized into datasets called dandisets.
Its files use [NWB](https://nwb.org), the standard format for neurophysiology, which is HDF5 underneath.
Each NWB file records what its experiment involved: the subject's species and genotype, the units that spike sorting found, and any stimulation devices.

DANDI also describes each dandiset with fields its submitters fill in, such as the list of experimental approaches.
Those fields summarize the whole dandiset, and they can leave out what a file contains.
Finding experiments of one kind reliably means reading the files themselves.

This example converts one NWB file from each of 11 dandisets into its own Icechunk repository, and asks which are mouse recordings with spike-sorted units and optogenetic stimulation.
The repositories sit on local disk under `dandi/`, named after their dandiset, such as `dandi/000009`, so `dandi/*` reads all 11.
`icechunk inspect nodes --decode-inline` prints one JSON line for every group and array, with the decoded values of small arrays such as each subject's species.
A DuckDB query, saved as `mouse_opto_units.sql`, groups those lines by repository and keeps the mouse recordings with a units table and an optogenetic stimulation site:

```console
$ icechunk inspect nodes dandi/* --decode-inline \
    | duckdb -table -f mouse_opto_units.sql
+----------+---------------+-------+
| dandiset |   genotype    | units |
+----------+---------------+-------+
| 000007   | unknown       | 6     |
| 000009   | Ai32 x PV-Cre | 3     |
+----------+---------------+-------+
```

Two recordings match, and the command did not fetch any chunk to find them.
DANDI's approach field for dandiset 000007 lists only electrophysiology, so a search of that field would miss it.

!!! tip "How the commands work"

    This page applies the row commands of `icechunk inspect` to real data and covers only what is specific to it. [Querying Many Repositories](querying-collections.md) teaches each command and the SQL used here, step by step, on a collection you build locally in seconds.

!!! warning "Prototype"

    The `icechunk inspect` command exists only on the `flatbuffer-queries` branch.

## Before you start

You need the `icechunk` CLI, jq and DuckDB, set up as in the main guide's [Before you start](querying-collections.md#before-you-start). Building the collection reads file headers from DANDI over the network and takes under a minute; follow the [appendix](#appendix-building-the-collection), then come back here. The commands below run in the directory the appendix creates.

## 1. The collection

The collection is 11 Icechunk repositories. Each holds one small NWB file, the recording sampled from one dandiset, so a result about `dandi/000009` is a claim about that one file, not the whole dandiset. They cover mice, rats, humans and rhesus macaques.

Each repository is an ordinary Icechunk repository with one commit. It holds the file's whole hierarchy of groups and arrays, with their attributes, and two kinds of chunks:

- **Small datasets**, such as the subject's species or a table of 3 units, are written into the repository. Many are small enough to be inline in the manifests, which is what lets `--decode-inline` read them.
- **Large datasets**, such as spike times, are [virtual references](virtual.md): byte ranges of the original NWB file in DANDI's bucket, `s3://dandiarchive/`. Reading them fetches those bytes from the bucket.

NWB lays out a recording as groups, and records each group's type in a `neurodata_type` attribute. Here is the repository for 000009, trimmed to the parts the queries use. Groups show their `neurodata_type`, and small arrays show their shape or value:

```text
/                            NWBFile
├── general
│   ├── subject              Subject
│   │   ├── species          "Mus musculus"
│   │   ├── genotype         "Ai32 x PV-Cre"
│   │   ├── sex              "M"
│   │   └── subject_id       "anm00260490"
│   ├── optogenetics
│   │   └── left-ALM         OptogeneticStimulusSite
│   └── devices, extracellular_ephys, institution, keywords, ...
├── intervals
│   └── trials               TimeIntervals: 17 columns, 270 trials
├── units                    Units
│   ├── id                   [3]
│   ├── spike_times          [9004], one virtual chunk
│   └── 10 more columns
└── acquisition, analysis, processing, specifications, stimulus, ...
```

The questions on this page use:

- **`/general/subject/species` and `/general/subject/genotype`**: `--decode-inline` reads these scalar strings.
- **`/units/id`**, whose length is the number of sorted units.
- **The `neurodata_type` attribute** of every group, to find an `OptogeneticStimulusSite`.
- **The commit metadata**, copied from the DANDI API when the collection was built: the dandiset id and version, the file's path, the species, and DANDI's list of experimental approaches for the dandiset.

## 2. Subjects across the collection

Every NWB file records its subject's species under `/general/subject`:

```console
$ icechunk inspect nodes dandi/* --decode-inline \
    | jq -r 'select(.path == "/general/subject/species")
             | "\(.repo)  \(.value)"' \
    | sort
dandi/000004  Human
dandi/000005  Mus musculus
dandi/000006  Mus musculus
dandi/000007  Mus musculus
dandi/000008  Mus musculus
dandi/000009  Mus musculus
dandi/000023  Homo Sapiens
dandi/000027  Rattus norvegicus
dandi/000059  Rattus norvegicus
dandi/000127  Macaca mulatta
dandi/000128  Macaca mulatta
```

The values are whatever each file's author wrote, so humans appear as both `Human` and `Homo Sapiens`. A query that compares species needs to allow for that, for example with `IN ('Human', 'Homo Sapiens')`.

These repositories are on local disk. In object storage, the cost would be request latency, as the main guide's [section 4](querying-collections.md#4-many-repositories-at-once) describes.

## 3. Mice with units and optogenetics

The question has three conditions, each about a different node of the same repository:

- **Mouse:** the value of `/general/subject/species`.
- **Spike-sorted units:** the length of `/units/id`, the first element of its `shape`. In SQL that is `shape[1]`, because DuckDB counts list positions from 1.
- **Optogenetic stimulation:** any group whose `neurodata_type` is `OptogeneticStimulusSite`.

The main guide builds this kind of query up in [steps](querying-collections.md#5-questions-across-rows-sql): one step per condition, joined by dandiset. One piece is new here: `value ->> '$'` turns a decoded `value` into text. DuckDB loads `value` as JSON, because its type differs from row to row. Save this as `mouse_opto_units.sql`:

```sql
WITH rows AS MATERIALIZED (FROM read_json('/dev/stdin')),
species AS (
    SELECT parse_filename(repo) AS dandiset, value ->> '$' AS species
    FROM rows WHERE path = '/general/subject/species'
),
genotype AS (
    SELECT parse_filename(repo) AS dandiset, value ->> '$' AS genotype
    FROM rows WHERE path = '/general/subject/genotype'
),
units AS (
    SELECT parse_filename(repo) AS dandiset, shape[1] AS units
    FROM rows WHERE path = '/units/id'
),
opto AS (
    SELECT DISTINCT parse_filename(repo) AS dandiset
    FROM rows WHERE attributes.neurodata_type = 'OptogeneticStimulusSite'
)
SELECT dandiset, genotype, units
FROM species JOIN units USING (dandiset) JOIN opto USING (dandiset)
     LEFT JOIN genotype USING (dandiset)
WHERE species = 'Mus musculus' AND units > 0
ORDER BY dandiset;
```

`LEFT JOIN` keeps a recording whose file does not record a genotype.

```console
$ icechunk inspect nodes dandi/* --decode-inline \
    | duckdb -table -f mouse_opto_units.sql
+----------+---------------+-------+
| dandiset |   genotype    | units |
+----------+---------------+-------+
| 000007   | unknown       | 6     |
| 000009   | Ai32 x PV-Cre | 3     |
+----------+---------------+-------+
```

The sampled files from 000005 and 000008 are mouse recordings without a units table, and the one from 000006 has a unit but does not have a stimulation site. The `unknown` genotype for 000007 is what its file records.

### 3.1 A file the approach field does not describe

Searching DANDI's approach field would not find 000007. The commit metadata holds the list of approaches DANDI reports for the dandiset:

```console
$ icechunk inspect commits dandi/000007 | jq -r '.metadata.approach[]'
electrophysiological approach
```

DANDI lists only electrophysiology, but the file contains a stimulation site:

```console
$ icechunk inspect nodes dandi/000007 \
    | jq -r 'select(.attributes.neurodata_type == "OptogeneticStimulusSite")
             | .path'
/general/optogenetics/Dentate
```

The approach field summarizes what the submitters declared for the whole dandiset. The row commands read what each file contains. This repository holds one file from the dandiset, so the contrast is between a dandiset-level summary and a fact about one file.

## 4. What a recording depends on: virtual chunks

The two recordings depend on files outside their repositories. Their virtual arrays are readable only while the source files exist in DANDI's bucket. `icechunk inspect chunks` gives each virtual chunk's location:

```console
$ icechunk inspect chunks dandi/000009 \
    | jq 'select(.path == "/units/spike_times")
          | {kind, length, offset, location}'
{
  "kind": "virtual",
  "length": 72032,
  "offset": 137232,
  "location": "s3://dandiarchive/blobs/fc9/181/fc91815a-98ce-4bcc-a369-7ebb89d3b34f"
}
```

The spike times are one chunk: 72,032 bytes starting at byte 137,232 of a file in DANDI's bucket. Summarize every chunk of both recordings. Save this as `bytes_behind.sql`:

```sql
SELECT parse_filename(repo)                             AS dandiset,
       count(*) FILTER (kind = 'virtual')               AS virtual,
       count(*) FILTER (kind = 'native')                AS native,
       count(*) FILTER (kind = 'inline')                AS inline,
       sum(length) FILTER (kind = 'virtual')            AS virtual_bytes,
       count(DISTINCT location)                         AS source_files
FROM read_json('/dev/stdin')
GROUP BY dandiset
ORDER BY dandiset;
```

```console
$ icechunk inspect chunks dandi/000007 dandi/000009 \
    | duckdb -table -f bytes_behind.sql
+----------+---------+--------+--------+---------------+--------------+
| dandiset | virtual | native | inline | virtual_bytes | source_files |
+----------+---------+--------+--------+---------------+--------------+
| 000007   | 1       | 26     | 39     | 132864        | 1            |
| 000009   | 1       | 32     | 37     | 72032         | 1            |
+----------+---------+--------+--------+---------------+--------------+
```

Each recording depends on one source file, through a single virtual chunk. Everything else, the metadata and small datasets, is stored in the repository.

If DANDI replaced that file, which repositories would break? Search every chunk row for its location. Here each source file is used by one repository, so the search finds only `000009`; in a collection where repositories share source files, the same query lists all of them:

```console
$ loc=s3://dandiarchive/blobs/fc9/181/fc91815a-98ce-4bcc-a369-7ebb89d3b34f
$ icechunk inspect chunks dandi/* \
    | jq -r --arg loc $loc 'select(.location == $loc) | "\(.repo)  \(.path)"'
dandi/000009  /units/spike_times
```

## 5. Reading the data

The query gave repository names and the rows gave array paths. Reading the spike times fetches their one virtual chunk from DANDI's bucket. Icechunk follows virtual references only into locations you authorize; the bucket is public, so anonymous credentials are enough. Save this as `open_spikes.py`:

```python
import icechunk
import zarr

repo = icechunk.Repository.open(
    icechunk.local_filesystem_storage("dandi/000009"),
    authorize_virtual_chunk_access=icechunk.containers_credentials(
        {"s3://dandiarchive/": icechunk.s3_anonymous_credentials()}
    ),
)
root = zarr.open_group(repo.readonly_session("main").store, mode="r")
spikes = root["units/spike_times"]
print(spikes.shape, spikes.dtype)
print(spikes[:5])
```

```console
$ uv run open_spikes.py
(9004,) float64
[1266.12898397 1266.49173307 1267.22444223 1267.84635972 1268.22434901]
```

## 6. What is specific to virtual collections

**Small virtual arrays are invisible to `--decode-inline`.** This collection's small datasets were stored in the repository, so their small chunks can be inline. A converter that leaves them as virtual references into the source file makes them invisible to `--decode-inline`, and the species query would not find any recordings.

**One manifest per array.** Each repository here has one manifest per array, about 70 for dandiset 000009, so `--decode-inline` makes one read per small array, as the main guide's [limits](querying-collections.md#10-what-the-commands-cannot-see) describe.

## Appendix: building the collection

The repositories are built with [zarrshadow](https://github.com/bendichter/zarrshadow). For each NWB file, zarrshadow reads the HDF5 headers over HTTP and describes the file as kerchunk-style JSON references (`generate_rfs`, for reference file system): a mapping from each Zarr key to metadata, to a chunk's bytes, or to a byte range of the file. At build time, `rfs_to_icechunk` converts those references into a new Icechunk repository. Chunk bytes become chunks in the repository; byte ranges become virtual chunks. The result is an ordinary Icechunk repository, and this page does not read the JSON references. The scripts pin zarrshadow to the commit the outputs on this page came from; later commits rename the project to shadowzarr.

The first script picks one small NWB file from each of 11 dandisets, using the DANDI API. The second builds one repository per file. Create a project, save the two scripts into it, and run them:

```bash
mkdir dandi-collection && cd dandi-collection
uv init --bare --python 3.12 && uv python pin 3.12
uv add "zarrshadow[icechunk] @ git+https://github.com/bendichter/zarrshadow@82e7673" \
    requests

uv run pick_assets.py > assets.json
uv run build_corpus.py assets.json dandi
```

<details markdown="1">
<summary><code>pick_assets.py</code>: choose one small NWB file from each dandiset</summary>

```python
"""Pick one small NWB file from each of 11 dandisets, spread over species."""

import json
import sys
from collections import defaultdict

import requests

API = "https://api.dandiarchive.org/api"
# NCBI taxon id -> how many dandisets to take
PER_SPECIES = {"10090": 5, "10116": 2, "9606": 2, "9544": 2}
MAX_BYTES = 300e6

picked = []
counts = defaultdict(int)
url = f"{API}/dandisets/?page_size=100&draft=false&empty=false&embargoed=false"
while url and len(picked) < sum(PER_SPECIES.values()):
    page = requests.get(url, timeout=60).json()
    url = page["next"]
    for d in page["results"]:
        v = d.get("most_recent_published_version")
        if not v:
            continue
        base = f"{API}/dandisets/{d['identifier']}/versions/{v['version']}"
        meta = requests.get(f"{base}/", timeout=60).json()
        summary = meta.get("assetsSummary", {})
        species = [
            s["identifier"].rsplit("_", 1)[-1]
            for s in summary.get("species", [])
        ]
        names = [s["name"] for s in summary.get("species", [])]
        if len(species) != 1 or species[0] not in PER_SPECIES:
            continue
        sp = species[0]
        if counts[sp] >= PER_SPECIES[sp]:
            continue
        assets = requests.get(
            f"{base}/assets/",
            params={"glob": "*.nwb", "page_size": 50},
            timeout=60,
        ).json()["results"]
        small = [a for a in assets if a["size"] < MAX_BYTES]
        if not small:
            continue
        a = min(small, key=lambda a: a["size"])
        counts[sp] += 1
        picked.append(
            {
                "dandiset": d["identifier"],
                "version": v["version"],
                "species": names[0],
                "taxon": sp,
                "approach": [
                    x.get("name") for x in summary.get("approach", [])
                ],
                "path": a["path"],
                "asset_id": a["asset_id"],
                "size": a["size"],
                "url": f"{API}/assets/{a['asset_id']}/download/",
            }
        )
        print(
            picked[-1]["dandiset"], sp, a["path"], a["size"], file=sys.stderr
        )
json.dump(picked, sys.stdout, indent=1)
```

</details>

<details markdown="1">
<summary><code>build_corpus.py</code>: build a repository for each file</summary>

```python
"""Build one Icechunk repository per picked NWB file, with zarrshadow."""

import json
import sys
import time
from pathlib import Path

import icechunk
import requests
from zarrshadow import generate_rfs
from zarrshadow.icechunk import rfs_to_icechunk

HTTPS = "https://dandiarchive.s3.amazonaws.com/"
S3 = "s3://dandiarchive/"

assets = json.loads(Path(sys.argv[1]).read_text())
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)

config = icechunk.RepositoryConfig.default()
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        S3, icechunk.s3_store(region="us-east-2", anonymous=True)
    )
)
credentials = icechunk.containers_credentials(
    {S3: icechunk.s3_anonymous_credentials()}
)

for a in assets:
    name = a["dandiset"]
    repo_dir = out / name
    if repo_dir.exists():
        continue
    blob = next(
        u
        for u in requests.get(
            f"https://api.dandiarchive.org/api/assets/{a['asset_id']}/",
            timeout=60,
        ).json()["contentUrl"]
        if u.startswith(HTTPS)
    )
    t0 = time.perf_counter()
    try:
        rfs = generate_rfs(blob)
        repo = icechunk.Repository.create(
            icechunk.local_filesystem_storage(str(repo_dir)),
            config,
            authorize_virtual_chunk_access=credentials,
        )
        session = repo.writable_session("main")
        rfs_to_icechunk(rfs, session, url_for=lambda u: u.replace(HTTPS, S3))
        session.commit(
            f"Virtualize {a['path']} from DANDI {name}",
            metadata={
                "dandiset": name,
                "version": a["version"],
                "asset_path": a["path"],
                "species": a["species"],
                "approach": a["approach"],
            },
        )
        seconds = time.perf_counter() - t0
        print(
            f"{name}: {len(rfs['refs'])} refs in {seconds:.1f}s",
            file=sys.stderr,
        )
    except Exception as e:
        print(f"{name}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
```

</details>
