//! Row commands: stream the metadata of many repositories as JSON lines.
//!
//! Every row comes from a snapshot or a manifest, never from a chunk object, so the
//! cost of a query grows with the number of repositories and arrays, not with the
//! size of the data. The output is meant to be piped into `jq`, or read by `DuckDB`
//! with `read_json('/dev/stdin')`.

use std::{
    collections::{HashMap, hash_map::Entry},
    io::{BufWriter, Stdout, Write as _},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use anyhow::{Context as _, Result};
use clap::{Args, Subcommand};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use itertools::Itertools as _;
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{
    Repository,
    cli::config::CliConfig,
    format::{
        ChunkIndices, Path, SnapshotId,
        manifest::{ChunkInfo, ChunkPayload, ChunkRef, VirtualChunkRef},
        snapshot::{NodeData, NodeSnapshot},
    },
    repository::VersionInfo,
    session::{Session, is_prefix_match},
};

use super::interface::{Access, open_any, parse_reference};

/// Arrays with more elements than this never get a `value`, even when their chunk
/// is inline.
const MAX_VALUE_ELEMENTS: u64 = 64;

#[derive(Debug, Subcommand)]
pub enum RowsCommand {
    #[command(name = "nodes", about = "Print one JSON line per group and array")]
    Nodes(NodesArgs),
    #[command(name = "chunks", about = "Print one JSON line per chunk reference")]
    Chunks(ChunksArgs),
    #[command(
        name = "commits",
        about = "Print one JSON line per repository with its commit message and metadata"
    )]
    Commits(CommitsArgs),
    #[command(
        name = "export",
        about = "Write one repository as a directory of JSON files that mirrors its hierarchy"
    )]
    Export(ExportArgs),
}

#[derive(Debug, Args)]
pub struct RepoSet {
    #[arg(
        name = "repos",
        help = "Repositories: config aliases, local paths, or s3://bucket/prefix URLs. \
                s3://bucket/prefix/* expands to every spec v2+ repository directly under prefix"
    )]
    repos: Vec<String>,
    #[arg(long, help = "File listing one repository per line, or - for stdin")]
    repos_from: Option<PathBuf>,
    #[arg(
        long = "ref",
        default_value = "main",
        help = "Branch, tag, or snapshot id to read in every repository"
    )]
    reference: String,
    #[command(flatten)]
    access: Access,
    #[arg(long, default_value_t = 16, help = "Repositories read at the same time")]
    concurrency: usize,
}

#[derive(Debug, Args)]
pub struct NodesArgs {
    #[command(flatten)]
    repos: RepoSet,
    #[arg(
        long,
        help = "Decode small single-chunk arrays whose chunk is inline in the manifest into a \
                `value`; reads one manifest per such array"
    )]
    decode_inline: bool,
}

#[derive(Debug, Args)]
pub struct ChunksArgs {
    #[command(flatten)]
    repos: RepoSet,
    #[arg(
        long,
        help = "Only arrays at or under this path, such as /a/b; other manifests are not read"
    )]
    path: Option<String>,
}

#[derive(Debug, Args)]
pub struct CommitsArgs {
    #[command(flatten)]
    repos: RepoSet,
}

#[derive(Debug, Args)]
pub struct ExportArgs {
    #[arg(name = "repo", help = "Config alias, local path, or s3://bucket/prefix URL")]
    repo: String,
    #[arg(name = "dir", help = "Directory to write; created if missing")]
    dir: PathBuf,
    #[arg(long = "ref", default_value = "main", help = "Branch, tag, or snapshot id")]
    reference: String,
    #[command(flatten)]
    access: Access,
    #[arg(long, help = "Only arrays at or under this path, such as /a/b")]
    path: Option<String>,
}

pub async fn run_rows(cmd: &RowsCommand, config: &CliConfig) -> Result<()> {
    match cmd {
        RowsCommand::Nodes(args) => {
            let decode_inline = args.decode_inline;
            for_each_repo(
                &args.repos,
                config,
                move |repo, repository, snapshot, out| async move {
                    let session = session_at(&repository, &snapshot).await?;
                    for row in
                        node_rows(&repo, &snapshot.to_string(), &session, decode_inline)
                            .await?
                    {
                        out.emit(&row)?;
                    }
                    Ok(())
                },
            )
            .await
        }
        RowsCommand::Chunks(args) => {
            let prefix = args.path.as_deref();
            for_each_repo(
                &args.repos,
                config,
                move |repo, repository, snapshot, out| async move {
                    let session = session_at(&repository, &snapshot).await?;
                    let snapshot = snapshot.to_string();
                    for_each_chunk(&session, prefix, |path, info| {
                        out.emit(&chunk_row(&repo, &snapshot, path, info))
                    })
                    .await
                },
            )
            .await
        }
        RowsCommand::Commits(args) => {
            for_each_repo(
                &args.repos,
                config,
                |repo, repository, snapshot, out| async move {
                    out.emit(&snapshot_row(&repo, &repository, &snapshot).await?)
                },
            )
            .await
        }
        RowsCommand::Export(args) => export(args, config).await,
    }
}

async fn session_at(repository: &Repository, snapshot: &SnapshotId) -> Result<Session> {
    Ok(repository.readonly_session(&VersionInfo::SnapshotId(snapshot.clone())).await?)
}

/// Shared stdout for repositories read concurrently. Each row is written whole under
/// the lock, so lines never tear, though rows of different repositories interleave.
struct Out(Mutex<BufWriter<Stdout>>);

impl Out {
    fn emit(&self, row: &impl Serialize) -> Result<()> {
        let mut line = serde_json::to_vec(row)?;
        line.push(b'\n');
        self.lock().write_all(&line)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.lock().flush()?;
        Ok(())
    }

    /// Every write is one whole line, so a panic while holding the lock cannot leave
    /// a partial row behind.
    fn lock(&self) -> MutexGuard<'_, BufWriter<Stdout>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A closed stdout, as when piping into `head`, means the reader has what it wants.
fn is_broken_pipe(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)
    })
}

/// Runs `rows` on every repository, streaming what it emits to stdout. A repository
/// that fails is reported on stderr and the rest still print.
async fn for_each_repo<F, Fut>(set: &RepoSet, config: &CliConfig, rows: F) -> Result<()>
where
    F: Fn(String, Repository, SnapshotId, Arc<Out>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut given = set.repos.clone();
    if let Some(path) = &set.repos_from {
        let listing = if path.as_os_str() == "-" {
            std::io::read_to_string(std::io::stdin()).context("Failed to read stdin")?
        } else {
            std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read {path:?}"))?
        };
        given.extend(
            listing.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from),
        );
    }
    let mut repos = Vec::with_capacity(given.len());
    for repo in given {
        match repo.strip_suffix("/*").and_then(|p| p.strip_prefix("s3://")) {
            Some(parent) => repos.extend(expand_s3_glob(parent, &set.access).await?),
            None => repos.push(repo),
        }
    }

    let out = Arc::new(Out(Mutex::new(BufWriter::new(std::io::stdout()))));
    let mut results = stream::iter(repos)
        .map(|repo| {
            let rows = &rows;
            let out = Arc::clone(&out);
            async move {
                let result = async {
                    let repository = open_any(&repo, config, &set.access).await?;
                    let snapshot = parse_reference(&repository, &set.reference).await?;
                    rows(repo.clone(), repository, snapshot, out).await
                }
                .await;
                (repo, result)
            }
        })
        .buffer_unordered(set.concurrency.max(1));

    let mut failed = 0;
    while let Some((repo, result)) = results.next().await {
        if let Err(err) = result {
            if is_broken_pipe(&err) {
                return Ok(());
            }
            failed += 1;
            eprintln!("{repo}: {err:#}");
        }
    }
    if let Err(err) = out.flush()
        && !is_broken_pipe(&err)
    {
        return Err(err);
    }
    if failed > 0 {
        let noun = if failed == 1 { "repository" } else { "repositories" };
        anyhow::bail!("{failed} {noun} could not be read");
    }
    Ok(())
}

/// Expands `bucket/prefix` (from `s3://bucket/prefix/*`) into the repositories
/// directly under it. A child prefix counts as a repository when it holds a `repo`
/// object, which every repository of spec version 2 or later has.
async fn expand_s3_glob(parent: &str, access: &Access) -> Result<Vec<String>> {
    let (bucket, prefix) = parent.split_once('/').unwrap_or((parent, ""));
    let prefix = match prefix.trim_end_matches('/') {
        "" => String::new(),
        p => format!("{p}/"),
    };
    let (options, credentials) = access.s3()?;
    let client = icechunk_s3::mk_client(
        &options,
        credentials,
        Vec::new(),
        Vec::new(),
        &icechunk_storage::Settings::default(),
    )
    .await;

    let mut children = Vec::new();
    let mut token = None;
    loop {
        let page = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .delimiter("/")
            .set_continuation_token(token)
            .send()
            .await
            .with_context(|| format!("Failed to list s3://{bucket}/{prefix}"))?;
        children.extend(
            page.common_prefixes().iter().filter_map(|p| p.prefix().map(String::from)),
        );
        token = page.next_continuation_token().map(String::from);
        if token.is_none() {
            break;
        }
    }

    // Keys sort lexicographically, so `{child}repo` comes first among keys
    // starting with it when it exists.
    let client = &client;
    let found: Vec<Option<String>> = stream::iter(children)
        .map(|child| async move {
            let key = format!("{child}repo");
            let page = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&key)
                .max_keys(1)
                .send()
                .await
                .with_context(|| format!("Failed to list s3://{bucket}/{child}"))?;
            let is_repo = page.contents().first().and_then(|o| o.key()) == Some(&key);
            anyhow::Ok(
                is_repo.then(|| format!("s3://{bucket}/{}", child.trim_end_matches('/'))),
            )
        })
        .buffered(16)
        .try_collect()
        .await?;
    let found: Vec<String> = found.into_iter().flatten().collect();
    eprintln!("s3://{parent}/* matched {} repositories", found.len());
    Ok(found)
}

async fn node_rows(
    repo: &str,
    snapshot: &str,
    session: &Session,
    decode_inline: bool,
) -> Result<Vec<Value>> {
    stream::iter(nodes_under(session, None).await?)
        .map(|node| async move {
            let zarr = parse_zarr(&node)?;
            let value = match decode_inline {
                true => small_array_value(session, &node, &zarr).await?,
                false => None,
            };
            let mut row = node_row(repo, snapshot, &node, zarr);
            if let Some(value) = value {
                row.insert("value".to_string(), value);
            }
            Ok(Value::Object(row))
        })
        .buffered(16)
        .try_collect()
        .await
}

fn parse_zarr(node: &NodeSnapshot) -> Result<Map<String, Value>> {
    serde_json::from_slice(&node.user_data)
        .with_context(|| format!("Invalid zarr.json at {}", node.path))
}

fn node_row(
    repo: &str,
    snapshot: &str,
    node: &NodeSnapshot,
    mut zarr: Map<String, Value>,
) -> Map<String, Value> {
    let attributes = zarr.remove("attributes").unwrap_or_else(|| json!({}));

    let mut row = Map::new();
    row.insert("repo".into(), repo.into());
    row.insert("snapshot".into(), snapshot.into());
    row.insert("path".into(), node.path.to_string().into());
    match &node.node_data {
        NodeData::Group => {
            row.insert("kind".into(), "group".into());
        }
        NodeData::Array { shape, dimension_names, .. } => {
            row.insert("kind".into(), "array".into());
            row.insert(
                "shape".into(),
                shape.iter().map(|d| d.array_length()).collect::<Vec<_>>().into(),
            );
            row.insert(
                "chunk_grid".into(),
                shape.num_chunks().collect::<Vec<_>>().into(),
            );
            row.insert(
                "dimension_names".into(),
                match dimension_names {
                    Some(names) => names
                        .iter()
                        .map(|n| Option::<String>::from(n.clone()))
                        .collect::<Vec<_>>()
                        .into(),
                    None => Value::Null,
                },
            );
        }
    }
    row.insert("attributes".into(), attributes);
    row.insert("zarr".into(), Value::Object(zarr));
    row
}

/// The decoded contents of an array small enough to be metadata in practice, such
/// as a scalar string. Only an inline chunk or an unwritten one (the fill value) is
/// used; a chunk stored elsewhere is never fetched.
async fn small_array_value(
    session: &Session,
    node: &NodeSnapshot,
    zarr: &Map<String, Value>,
) -> Result<Option<Value>> {
    let NodeData::Array { shape, .. } = &node.node_data else { return Ok(None) };
    let elements: u64 = shape.iter().map(|d| d.array_length()).product();
    if elements > MAX_VALUE_ELEMENTS || shape.num_chunks().any(|n| n > 1) {
        return Ok(None);
    }
    let coords = ChunkIndices(vec![0; shape.len()]);
    let values = match session.get_chunk_ref(&node.path, &coords).await? {
        Some(ChunkPayload::Inline(bytes)) => {
            decode_chunk(zarr, &bytes, elements as usize)
        }
        None => return Ok(zarr.get("fill_value").cloned()),
        Some(_) => None,
    };
    Ok(values
        .map(|mut v| if shape.is_empty() { v.swap_remove(0) } else { Value::from(v) }))
}

/// Decodes a chunk whose codec pipeline is a `vlen-utf8` or `bytes` codec,
/// optionally followed by `zstd`: the pipelines zarr-python writes small strings
/// and numbers with. Other pipelines are left undecoded.
fn decode_chunk(
    zarr: &Map<String, Value>,
    bytes: &[u8],
    elements: usize,
) -> Option<Vec<Value>> {
    let codecs = zarr.get("codecs")?.as_array()?.as_slice();
    let (codec, bytes) = match codecs {
        [codec] => (codec, std::borrow::Cow::Borrowed(bytes)),
        [codec, compressor] if compressor.get("name")? == "zstd" => {
            (codec, std::borrow::Cow::Owned(zstd::decode_all(bytes).ok()?))
        }
        _ => return None,
    };
    match codec.get("name")?.as_str()? {
        "vlen-utf8" => decode_vlen_utf8(&bytes, elements),
        "bytes" => {
            let big_endian =
                codec.pointer("/configuration/endian") == Some(&Value::from("big"));
            decode_numbers(zarr.get("data_type")?.as_str()?, big_endian, &bytes, elements)
        }
        _ => None,
    }
}

/// `vlen-utf8` layout: a little-endian u32 item count, then each item as a u32
/// byte length followed by its UTF-8 bytes.
fn decode_vlen_utf8(bytes: &[u8], elements: usize) -> Option<Vec<Value>> {
    let read_u32 = |at: usize| -> Option<usize> {
        Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?) as usize)
    };
    let count = read_u32(0)?;
    let mut at = 4;
    let mut out = Vec::with_capacity(count.min(elements));
    for _ in 0..count.min(elements) {
        let len = read_u32(at)?;
        at += 4;
        out.push(Value::from(std::str::from_utf8(bytes.get(at..at + len)?).ok()?));
        at += len;
    }
    Some(out)
}

fn decode_numbers(
    data_type: &str,
    big_endian: bool,
    bytes: &[u8],
    elements: usize,
) -> Option<Vec<Value>> {
    macro_rules! decode {
        ($t:ty, $to_value:expr) => {{
            const SIZE: usize = std::mem::size_of::<$t>();
            let data = bytes.get(..elements * SIZE)?;
            data.chunks_exact(SIZE)
                .map(|c| {
                    let arr: [u8; SIZE] = c.try_into().ok()?;
                    let n = if big_endian {
                        <$t>::from_be_bytes(arr)
                    } else {
                        <$t>::from_le_bytes(arr)
                    };
                    Some($to_value(n))
                })
                .collect()
        }};
    }
    let float =
        |f: f64| serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number);
    match data_type {
        "bool" => bytes
            .get(..elements)
            .map(|b| b.iter().map(|&x| Value::from(x != 0)).collect()),
        "int8" => decode!(i8, Value::from),
        "int16" => decode!(i16, Value::from),
        "int32" => decode!(i32, Value::from),
        "int64" => decode!(i64, Value::from),
        "uint8" => decode!(u8, Value::from),
        "uint16" => decode!(u16, Value::from),
        "uint32" => decode!(u32, Value::from),
        "uint64" => decode!(u64, Value::from),
        "float32" => decode!(f32, |f: f32| float(f as f64)),
        "float64" => decode!(f64, float),
        _ => None,
    }
}

/// Every node at or under `prefix`, or every node when there is none.
async fn nodes_under(
    session: &Session,
    prefix: Option<&str>,
) -> Result<Vec<NodeSnapshot>> {
    let prefix = prefix.map_or("", |p| p.trim_end_matches('/'));
    Ok(session
        .list_nodes(&Path::root())
        .await?
        .filter_ok(|n| is_prefix_match(n.path.as_str(), prefix))
        .collect::<Result<_, _>>()?)
}

/// Calls `f` for every chunk reference of every array at or under `prefix`, reading
/// only those arrays' manifests, one array at a time.
async fn for_each_chunk(
    session: &Session,
    prefix: Option<&str>,
    mut f: impl FnMut(&Path, &ChunkInfo) -> Result<()>,
) -> Result<()> {
    for node in nodes_under(session, prefix).await? {
        if !matches!(node.node_data, NodeData::Array { .. }) {
            continue;
        }
        let mut chunks = Box::pin(session.all_node_chunks(&node).await);
        while let Some(item) = chunks.next().await {
            let (path, info) = item?;
            f(&path, &info)?;
        }
    }
    Ok(())
}

fn chunk_row(repo: &str, snapshot: &str, path: &Path, info: &ChunkInfo) -> Value {
    let mut row = chunk_fields(info);
    row["repo"] = repo.into();
    row["snapshot"] = snapshot.into();
    row["path"] = path.to_string().into();
    row
}

/// What a manifest says about one chunk, without the repository and array it
/// belongs to.
fn chunk_fields(info: &ChunkInfo) -> Value {
    let mut fields = json!({"coord": info.coord.0, "length": info.payload.length()});
    match &info.payload {
        ChunkPayload::Inline(_) => fields["kind"] = "inline".into(),
        ChunkPayload::Virtual(VirtualChunkRef { location, offset, .. }) => {
            fields["kind"] = "virtual".into();
            fields["location"] = location.url().into();
            fields["offset"] = (*offset).into();
        }
        ChunkPayload::Ref(ChunkRef { id, offset, .. }) => {
            fields["kind"] = "native".into();
            fields["chunk_id"] = id.to_string().into();
            fields["offset"] = (*offset).into();
        }
        _ => fields["kind"] = "unknown".into(),
    }
    fields
}

async fn snapshot_row(
    repo: &str,
    repository: &Repository,
    snapshot: &SnapshotId,
) -> Result<Value> {
    let info = repository.lookup_snapshot(snapshot).await?;
    Ok(json!({
        "repo": repo,
        "snapshot": info.id.to_string(),
        "flushed_at": info.flushed_at,
        "message": info.message,
        "metadata": info.metadata,
    }))
}

/// Writes `snapshot.json` at the top of `dir`, then for every node a directory at
/// its path holding its `zarr.json`, and for every array a `chunks.ndjson` with one
/// line per chunk reference. Small inline arrays also get a `value.json`.
async fn export(args: &ExportArgs, config: &CliConfig) -> Result<()> {
    let repository = open_any(&args.repo, config, &args.access).await?;
    let snapshot = parse_reference(&repository, &args.reference).await?;
    let session = session_at(&repository, &snapshot).await?;
    let node_dir = |path: &str| args.dir.join(path.trim_start_matches('/'));

    write_json(
        &args.dir.join("snapshot.json"),
        &snapshot_row(&args.repo, &repository, &snapshot).await?,
    )?;
    for node in nodes_under(&session, args.path.as_deref()).await? {
        let dir = node_dir(node.path.as_str());
        let zarr = parse_zarr(&node)?;
        write_json(&dir.join("zarr.json"), &zarr)?;
        if let Some(value) = small_array_value(&session, &node, &zarr).await? {
            write_json(&dir.join("value.json"), &value)?;
        }
    }

    let mut files: HashMap<String, BufWriter<std::fs::File>> = HashMap::new();
    for_each_chunk(&session, args.path.as_deref(), |path, info| {
        let row = chunk_fields(info);
        let file = match files.entry(path.to_string()) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let file =
                    std::fs::File::create(node_dir(e.key()).join("chunks.ndjson"))?;
                e.insert(BufWriter::new(file))
            }
        };
        serde_json::to_writer(&mut *file, &row)?;
        file.write_all(b"\n")?;
        Ok(())
    })
    .await?;
    for file in files.values_mut() {
        file.flush()?;
    }
    eprintln!("Wrote {} at snapshot {snapshot} to {:?}", args.repo, args.dir);
    Ok(())
}

fn write_json(path: &std::path::Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(value)? + "\n")
        .with_context(|| format!("Failed to write {path:?}"))
}
