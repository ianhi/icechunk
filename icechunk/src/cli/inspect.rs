//! Inspect commands: print a repository's format files as JSON, either one file as
//! a document or the metadata of many repositories as JSON lines.

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{
    cli::config::CliConfig,
    format::ManifestId,
    inspect::{manifest_json, repo_info_json, snapshot_json, transaction_log_json},
};

use super::{
    interface::{Access, open_any, parse_reference},
    rows::{RowsCommand, run_rows},
};

#[derive(Debug, Subcommand)]
pub enum InspectCommand {
    #[command(
        name = "repo",
        about = "Print the repo info file: branches, tags, snapshots"
    )]
    Repo(InspectRepoArgs),
    #[command(name = "snapshot", about = "Print a snapshot file: nodes and manifests")]
    Snapshot(InspectRefArgs),
    #[command(name = "txlog", about = "Print the transaction log of a snapshot")]
    TxLog(InspectRefArgs),
    #[command(name = "manifest", about = "Print a summary of a manifest file")]
    Manifest(InspectManifestArgs),
    #[command(flatten)]
    Rows(RowsCommand),
}

#[derive(Debug, Args)]
pub struct InspectRepoArgs {
    #[arg(name = "repo", help = "Config alias, local path, or s3://bucket/prefix URL")]
    repo: String,
    #[command(flatten)]
    access: Access,
}

#[derive(Debug, Args)]
pub struct InspectRefArgs {
    #[command(flatten)]
    repo: InspectRepoArgs,
    #[arg(long = "ref", default_value = "main", help = "Branch, tag, or snapshot id")]
    reference: String,
}

#[derive(Debug, Args)]
pub struct InspectManifestArgs {
    #[command(flatten)]
    repo: InspectRepoArgs,
    #[arg(name = "manifest_id", help = "Manifest id, as listed by `inspect snapshot`")]
    manifest_id: String,
}

pub async fn run_inspect(cmd: &InspectCommand, config: &CliConfig) -> Result<()> {
    let json = match cmd {
        InspectCommand::Rows(rows) => return run_rows(rows, config).await,
        InspectCommand::Repo(args) => {
            let repository = open_any(&args.repo, config, &args.access).await?;
            repo_info_json(repository.asset_manager(), true).await?
        }
        InspectCommand::Snapshot(args) => {
            let repository = open_any(&args.repo.repo, config, &args.repo.access).await?;
            let id = parse_reference(&repository, &args.reference).await?;
            snapshot_json(repository.asset_manager(), &id, true).await?
        }
        InspectCommand::TxLog(args) => {
            let repository = open_any(&args.repo.repo, config, &args.repo.access).await?;
            let id = parse_reference(&repository, &args.reference).await?;
            transaction_log_json(repository.asset_manager(), &id, true).await?
        }
        InspectCommand::Manifest(args) => {
            let repository = open_any(&args.repo.repo, config, &args.repo.access).await?;
            let id = ManifestId::try_from(args.manifest_id.as_str())
                .map_err(|e| anyhow::anyhow!("Invalid manifest id: {e}"))?;
            manifest_json(repository.asset_manager(), &id, true).await?
        }
    };
    println!("{json}");
    Ok(())
}
