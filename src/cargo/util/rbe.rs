use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use cargo_util::{ProcessBuilder, ProcessError, paths};
use futures_util::stream;
use prost::Message;
use rand::RngExt;
use sha2::{Digest as _, Sha256};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::Endpoint;
use tonic::Request;

use crate::core::PackageId;
use crate::core::Target;
use crate::core::compiler::{
    CompileMode, DefaultExecutor, Executor, ExecutorContext, RemoteBuildConfig,
    RemoteExecutorContext,
};
use crate::util::CargoResult;

mod proto {
    pub mod build {
        pub mod bazel {
            pub mod remote {
                pub mod execution {
                    pub mod v2 {
                        #![allow(dead_code)]
                        tonic::include_proto!("build.bazel.remote.execution.v2");
                    }
                }
            }
        }
    }

    pub mod google {
        pub mod bytestream {
            #![allow(dead_code)]
            tonic::include_proto!("google.bytestream");
        }
        pub mod longrunning {
            #![allow(dead_code)]
            tonic::include_proto!("google.longrunning");
        }
        pub mod rpc {
            #![allow(dead_code)]
            tonic::include_proto!("google.rpc");
        }
    }
}

use proto::build::bazel::remote::execution::v2 as repb;
use proto::google::bytestream::byte_stream_client::ByteStreamClient;
use repb::content_addressable_storage_client::ContentAddressableStorageClient;
use repb::execution_client::ExecutionClient;

const WORKDIR_NAME: &str = "workspace";
const AUX_ROOT: &str = ".cargo-rbe";
const INLINE_BLOB_LIMIT: u64 = 8 * 1024 * 1024;
const MAX_BATCH_BLOBS: usize = 128;
const MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const BYTESTREAM_CHUNK_SIZE: usize = 1_000_000;

#[derive(Debug)]
pub struct RemoteExecutor {
    config: RemoteBuildConfig,
}

impl RemoteExecutor {
    pub fn new(config: RemoteBuildConfig) -> Self {
        Self { config }
    }

    fn exec_remote(
        &self,
        context: &RemoteExecutorContext,
        cmd: &ProcessBuilder,
        on_stdout_line: &mut dyn FnMut(&str) -> CargoResult<()>,
        on_stderr_line: &mut dyn FnMut(&str) -> CargoResult<()>,
    ) -> CargoResult<()> {
        let prepared = PreparedCommand::new(context, cmd, &self.config)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create runtime for remote execution")?;

        let result = rt.block_on(self.execute(prepared))?;

        emit_lines(&result.stdout, on_stdout_line)?;
        emit_lines(&result.stderr, on_stderr_line)?;

        if let Some(status) = result.status {
            if status.code != 0 {
                bail!(
                    "remote execution failed with status {}: {}",
                    status.code,
                    status.message
                );
            }
        }

        if result.exit_code == 0 {
            Ok(())
        } else {
            Err(ProcessError::new_raw(
                &format!("process didn't exit successfully: {}", cmd),
                Some(result.exit_code),
                &format!("exit code {}", result.exit_code),
                Some(&result.stdout),
                Some(&result.stderr),
            )
            .into())
        }
    }

    async fn execute(&self, prepared: PreparedCommand) -> CargoResult<RemoteResult> {
        let channel = Endpoint::new(self.config.endpoint.clone())
            .context("invalid `build.rbe.endpoint`")?
            .connect()
            .await
            .with_context(|| format!("failed to connect to {}", self.config.endpoint))?;
        let mut cas = ContentAddressableStorageClient::new(channel.clone())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let mut bs = ByteStreamClient::new(channel.clone())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let mut exec = ExecutionClient::new(channel.clone())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        self.upload_missing_blobs(&mut cas, &mut bs, &prepared.uploads)
            .await?;

        let execute_req = repb::ExecuteRequest {
            instance_name: self.config.instance_name.clone(),
            skip_cache_lookup: !self.config.remote_cache,
            action_digest: Some(prepared.action_digest.clone()),
            inline_stdout: true,
            inline_stderr: true,
        };
        let mut stream = exec
            .execute(self.request(execute_req)?)
            .await
            .context("failed to start remote execution")?
            .into_inner();

        let execute_response = loop {
            let Some(operation) = stream
                .message()
                .await
                .context("failed while polling remote execution")?
            else {
                bail!("remote execution stream ended before completion");
            };

            if !operation.done {
                continue;
            }

            match operation.result {
                Some(proto::google::longrunning::operation::Result::Response(any)) => {
                    break unpack_execute_response(any)?;
                }
                Some(proto::google::longrunning::operation::Result::Error(status)) => {
                    bail!(
                        "remote execution operation failed with status {}: {}",
                        status.code,
                        status.message
                    );
                }
                None => bail!("remote execution completed without a response"),
            }
        };

        let result = execute_response
            .result
            .context("remote execution response did not include an action result")?;

        let stdout = self
            .download_blob_if_needed(
                &mut cas,
                &mut bs,
                &result.stdout_raw,
                result.stdout_digest.as_ref(),
            )
            .await?;
        let stderr = self
            .download_blob_if_needed(
                &mut cas,
                &mut bs,
                &result.stderr_raw,
                result.stderr_digest.as_ref(),
            )
            .await?;

        self.materialize_outputs(&mut cas, &mut bs, &prepared.expected_outputs, &result)
            .await?;

        Ok(RemoteResult {
            exit_code: result.exit_code,
            stdout,
            stderr,
            status: execute_response.status,
        })
    }

    async fn upload_missing_blobs(
        &self,
        cas: &mut ContentAddressableStorageClient<tonic::transport::Channel>,
        bs: &mut ByteStreamClient<tonic::transport::Channel>,
        uploads: &UploadMap,
    ) -> CargoResult<()> {
        let digests: Vec<_> = uploads.values().map(|blob| blob.digest.clone()).collect();
        let missing = self.find_missing_blobs(cas, &digests).await?;

        let mut pending_batch = Vec::new();
        let mut pending_bytes = 0_u64;

        for key in missing {
            let Some(blob) = uploads.get(&key) else {
                continue;
            };
            if blob.digest.size_bytes == 0 {
                continue;
            }

            let data = blob.read_bytes()?;
            let data_len = data.len() as u64;
            if data_len > MAX_BATCH_BYTES {
                if !pending_batch.is_empty() {
                    self.batch_update(cas, std::mem::take(&mut pending_batch))
                        .await?;
                    pending_bytes = 0;
                }
                self.bytestream_upload(bs, blob, data).await?;
                continue;
            }
            if pending_batch.len() >= MAX_BATCH_BLOBS || pending_bytes + data_len > MAX_BATCH_BYTES
            {
                self.batch_update(cas, std::mem::take(&mut pending_batch))
                    .await?;
                pending_bytes = 0;
            }

            pending_bytes += data_len;
            pending_batch.push(repb::batch_update_blobs_request::Request {
                digest: Some(blob.digest.clone()),
                data,
            });
        }

        if !pending_batch.is_empty() {
            self.batch_update(cas, pending_batch).await?;
        }

        Ok(())
    }

    async fn find_missing_blobs(
        &self,
        cas: &mut ContentAddressableStorageClient<tonic::transport::Channel>,
        digests: &[repb::Digest],
    ) -> CargoResult<BTreeSet<DigestKey>> {
        let mut missing = BTreeSet::new();

        for chunk in digests.chunks(MAX_BATCH_BLOBS) {
            let response = cas
                .find_missing_blobs(self.request(repb::FindMissingBlobsRequest {
                    instance_name: self.config.instance_name.clone(),
                    blob_digests: chunk.to_vec(),
                })?)
                .await
                .context("failed to query the remote CAS for missing blobs")?
                .into_inner();

            for digest in response.missing_blob_digests {
                missing.insert(DigestKey::from_digest(&digest));
            }
        }

        Ok(missing)
    }

    async fn batch_update(
        &self,
        cas: &mut ContentAddressableStorageClient<tonic::transport::Channel>,
        requests: Vec<repb::batch_update_blobs_request::Request>,
    ) -> CargoResult<()> {
        let response = cas
            .batch_update_blobs(self.request(repb::BatchUpdateBlobsRequest {
                instance_name: self.config.instance_name.clone(),
                requests,
            })?)
            .await
            .context("failed to upload blobs to the remote CAS")?
            .into_inner();

        for response in response.responses {
            if let Some(status) = response.status {
                if status.code != 0 {
                    bail!(
                        "remote CAS rejected blob upload with status {}: {}",
                        status.code,
                        status.message
                    );
                }
            }
        }

        Ok(())
    }

    async fn download_blob_if_needed(
        &self,
        cas: &mut ContentAddressableStorageClient<tonic::transport::Channel>,
        bs: &mut ByteStreamClient<tonic::transport::Channel>,
        inline: &[u8],
        digest: Option<&repb::Digest>,
    ) -> CargoResult<Vec<u8>> {
        if !inline.is_empty() {
            return Ok(inline.to_vec());
        }

        let Some(digest) = digest else {
            return Ok(Vec::new());
        };
        if digest.size_bytes == 0 {
            return Ok(Vec::new());
        }
        if digest.size_bytes as u64 > MAX_BATCH_BYTES {
            return self.bytestream_download(bs, digest).await;
        }

        let response = cas
            .batch_read_blobs(self.request(repb::BatchReadBlobsRequest {
                instance_name: self.config.instance_name.clone(),
                digests: vec![digest.clone()],
            })?)
            .await
            .context("failed to download blob from the remote CAS")?
            .into_inner();

        let Some(response) = response.responses.into_iter().next() else {
            bail!("remote CAS returned no data for blob {}", digest.hash);
        };
        if let Some(status) = response.status {
            if status.code != 0 {
                bail!(
                    "remote CAS returned status {} while downloading blob {}: {}",
                    status.code,
                    digest.hash,
                    status.message
                );
            }
        }
        Ok(response.data)
    }

    async fn bytestream_upload(
        &self,
        bs: &mut ByteStreamClient<tonic::transport::Channel>,
        blob: &UploadBlob,
        data: Vec<u8>,
    ) -> CargoResult<()> {
        let resource_name = upload_resource_name(&self.config.instance_name, &blob.digest);
        let len = data.len();
        let requests = data
            .chunks(BYTESTREAM_CHUNK_SIZE)
            .enumerate()
            .map(|(index, chunk)| proto::google::bytestream::WriteRequest {
                resource_name: if index == 0 {
                    resource_name.clone()
                } else {
                    String::new()
                },
                write_offset: (index * BYTESTREAM_CHUNK_SIZE) as i64,
                data: chunk.to_vec(),
                finish_write: (index + 1) * BYTESTREAM_CHUNK_SIZE >= len,
            })
            .collect::<Vec<_>>();

        let response = bs
            .write(self.request(stream::iter(requests))?)
            .await
            .context("failed to upload blob via ByteStream")?
            .into_inner();

        let committed_size = response.committed_size;

        if committed_size != blob.digest.size_bytes {
            bail!(
                "ByteStream committed {} bytes for blob {}, expected {}",
                committed_size,
                blob.digest.hash,
                blob.digest.size_bytes
            );
        }

        Ok(())
    }
    async fn bytestream_download(
        &self,
        bs: &mut ByteStreamClient<tonic::transport::Channel>,
        digest: &repb::Digest,
    ) -> CargoResult<Vec<u8>> {
        let mut stream = bs
            .read(self.request(proto::google::bytestream::ReadRequest {
                resource_name: download_resource_name(&self.config.instance_name, digest),
                read_offset: 0,
                read_limit: 0,
            })?)
            .await
            .context("failed to start ByteStream download")?
            .into_inner();

        let mut data = Vec::with_capacity(digest.size_bytes.max(0) as usize);
        while let Some(response) = stream
            .message()
            .await
            .context("failed while downloading from ByteStream")?
        {
            data.extend_from_slice(&response.data);
        }
        Ok(data)
    }

    async fn materialize_outputs(
        &self,
        cas: &mut ContentAddressableStorageClient<tonic::transport::Channel>,
        bs: &mut ByteStreamClient<tonic::transport::Channel>,
        expected_outputs: &BTreeMap<String, PathBuf>,
        result: &repb::ActionResult,
    ) -> CargoResult<()> {
        let mut actual = BTreeMap::new();
        for output in &result.output_files {
            actual.insert(output.path.clone(), OutputMaterialization::File(output));
        }
        for output in &result.output_symlinks {
            actual.insert(output.path.clone(), OutputMaterialization::Symlink(output));
        }

        for local in expected_outputs.values() {
            remove_existing_output(local)?;
        }

        for (remote, local) in expected_outputs {
            let Some(actual) = actual.get(remote) else {
                continue;
            };
            if let Some(parent) = local.parent() {
                paths::create_dir_all(parent)?;
            }
            match actual {
                OutputMaterialization::File(output) => {
                    let data = if !output.contents.is_empty() {
                        output.contents.clone()
                    } else {
                        self.download_blob_if_needed(cas, bs, &[], output.digest.as_ref())
                            .await?
                    };
                    fs::write(local, data)?;
                    set_executable(local, output.is_executable)?;
                }
                OutputMaterialization::Symlink(output) => write_symlink(local, &output.target)?,
            }
        }

        Ok(())
    }

    fn request<T>(&self, message: T) -> CargoResult<Request<T>> {
        let mut request = Request::new(message);
        let metadata = request.metadata_mut();

        if let Some(api_key) = &self.config.api_key {
            let value = MetadataValue::try_from(api_key.as_str())
                .context("invalid `build.rbe.api-key` value")?;
            metadata.insert("x-buildbuddy-api-key", value);
        }

        for (name, value) in &self.config.headers {
            let key = MetadataKey::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid `build.rbe.headers` name `{name}`"))?;
            let value = MetadataValue::try_from(value.as_str())
                .with_context(|| format!("invalid `build.rbe.headers` value for `{name}`"))?;
            metadata.insert(key, value);
        }

        Ok(request)
    }
}

impl Executor for RemoteExecutor {
    fn exec(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        target: &Target,
        mode: CompileMode,
        on_stdout_line: &mut dyn FnMut(&str) -> CargoResult<()>,
        on_stderr_line: &mut dyn FnMut(&str) -> CargoResult<()>,
    ) -> CargoResult<()> {
        DefaultExecutor.exec(cmd, id, target, mode, on_stdout_line, on_stderr_line)
    }

    fn exec_with_context(
        &self,
        context: Option<&ExecutorContext>,
        cmd: &ProcessBuilder,
        id: PackageId,
        target: &Target,
        mode: CompileMode,
        on_stdout_line: &mut dyn FnMut(&str) -> CargoResult<()>,
        on_stderr_line: &mut dyn FnMut(&str) -> CargoResult<()>,
    ) -> CargoResult<()> {
        let Some(context) = context.and_then(|context| context.remote.as_ref()) else {
            bail!("remote execution requires an executor context");
        };

        match self.exec_remote(context, cmd, on_stdout_line, on_stderr_line) {
            Ok(()) => Ok(()),
            Err(err) if self.config.fallback_local => {
                tracing::warn!("remote execution failed, falling back to local: {err:#}");
                DefaultExecutor.exec(cmd, id, target, mode, on_stdout_line, on_stderr_line)
            }
            Err(err) => Err(err),
        }
    }
}

struct RemoteResult {
    exit_code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status: Option<proto::google::rpc::Status>,
}

struct PreparedCommand {
    action_digest: repb::Digest,
    uploads: UploadMap,
    expected_outputs: BTreeMap<String, PathBuf>,
}

impl PreparedCommand {
    fn new(
        context: &RemoteExecutorContext,
        cmd: &ProcessBuilder,
        config: &RemoteBuildConfig,
    ) -> CargoResult<Self> {
        let cwd = cmd
            .get_cwd()
            .context("remote execution requires a process cwd")?
            .to_path_buf();
        let resolved_program = resolve_program_path(cmd, Path::new(cmd.get_program()))?;
        let expected_program = resolve_program_path(cmd, &context.rustc_path)
            .unwrap_or_else(|_| context.rustc_path.clone());
        if resolved_program != expected_program {
            bail!("remote execution only supports direct rustc invocations");
        }

        let mut mapper = PathMapper::new(
            cwd,
            context.target_dir.clone(),
            context.build_dir.clone(),
            context.sysroot.clone(),
            context.outputs.iter().cloned().collect(),
        )?;

        mapper.add_package_tree(&context.package_root)?;
        mapper.add_input_path(&resolved_program)?;

        let program = mapper.map_input_command_path(&resolved_program)?;
        let args = rewrite_args(&mut mapper, cmd)?;
        let environment_variables = rewrite_envs(&mut mapper, cmd)?;
        let expected_outputs = build_expected_outputs(&mut mapper, &context.outputs)?;

        let command = repb::Command {
            arguments: std::iter::once(program).chain(args).collect(),
            environment_variables,
            platform: Some(platform_from_config(config)),
            working_directory: WORKDIR_NAME.to_owned(),
            output_paths: expected_outputs.keys().cloned().collect(),
            output_directory_format: repb::command::OutputDirectoryFormat::TreeAndDirectory as i32,
        };

        let command_bytes = command.encode_to_vec();
        let command_digest = digest_bytes(&command_bytes);
        mapper.add_blob(command_digest.clone(), UploadData::Bytes(command_bytes));

        let action = repb::Action {
            command_digest: Some(command_digest),
            input_root_digest: Some(mapper.root_digest()?),
            do_not_cache: !config.remote_cache,
            platform: Some(platform_from_config(config)),
        };
        let action_bytes = action.encode_to_vec();
        let action_digest = digest_bytes(&action_bytes);
        mapper.add_blob(action_digest.clone(), UploadData::Bytes(action_bytes));

        Ok(Self {
            action_digest,
            uploads: mapper.uploads,
            expected_outputs,
        })
    }
}

fn rewrite_args(mapper: &mut PathMapper, cmd: &ProcessBuilder) -> CargoResult<Vec<String>> {
    let args = utf8_args(cmd)?;
    let mut rewritten = Vec::with_capacity(args.len());

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--out-dir" | "-o" => {
                let next = args
                    .get(i + 1)
                    .context("expected a value after a path-bearing rustc flag")?;
                rewritten.push(arg.clone());
                rewritten.push(mapper.map_output_command_path(&mapper.resolve(next))?);
                i += 2;
                continue;
            }
            "--extern" => {
                let next = args
                    .get(i + 1)
                    .context("expected a value after `--extern`")?;
                rewritten.push(arg.clone());
                rewritten.push(rewrite_named_path_value(mapper, next)?);
                i += 2;
                continue;
            }
            "-L" => {
                let next = args.get(i + 1).context("expected a value after `-L`")?;
                rewritten.push(arg.clone());
                rewritten.push(rewrite_search_path_value(mapper, next)?);
                i += 2;
                continue;
            }
            "-C" => {
                let next = args.get(i + 1).context("expected a value after `-C`")?;
                if next.starts_with("incremental=") {
                    i += 2;
                    continue;
                }
                rewritten.push(arg.clone());
                rewritten.push(rewrite_codegen_arg(mapper, next)?);
                i += 2;
                continue;
            }
            "--sysroot" => {
                let next = args
                    .get(i + 1)
                    .context("expected a value after `--sysroot`")?;
                rewritten.push(arg.clone());
                rewritten.push(mapper.map_input_command_path(&mapper.resolve(next))?);
                i += 2;
                continue;
            }
            _ => {}
        }

        if let Some(value) = arg.strip_prefix("--sysroot=") {
            rewritten.push(format!(
                "--sysroot={}",
                mapper.map_input_command_path(&mapper.resolve(value))?
            ));
        } else if let Some(value) = arg.strip_prefix("--remap-path-prefix=") {
            rewritten.push(rewrite_remap_path_prefix(mapper, value)?);
        } else if let Some(path) = maybe_existing_path(mapper, arg) {
            rewritten.push(mapper.map_input_command_path(&path)?);
        } else {
            rewritten.push(arg.clone());
        }

        i += 1;
    }

    Ok(rewritten)
}

fn rewrite_envs(
    mapper: &mut PathMapper,
    cmd: &ProcessBuilder,
) -> CargoResult<Vec<repb::command::EnvironmentVariable>> {
    let mut envs = Vec::new();

    for (name, value) in cmd.get_envs() {
        let Some(value) = value else {
            continue;
        };
        let value = os_to_utf8(value)?;
        let value = rewrite_env_value(mapper, &value)?;
        envs.push(repb::command::EnvironmentVariable {
            name: name.clone(),
            value,
        });
    }

    envs.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(envs)
}

fn rewrite_env_value(mapper: &mut PathMapper, value: &str) -> CargoResult<String> {
    let as_path = mapper.resolve(value);
    if as_path.exists() {
        return mapper.map_input_command_path(&as_path);
    }

    let split_paths: Vec<_> = std::env::split_paths(&OsString::from(value)).collect();
    if !split_paths.is_empty() && split_paths.iter().all(|path| path.exists()) {
        let mapped = split_paths
            .iter()
            .map(|path| mapper.map_input_command_path(path))
            .collect::<CargoResult<Vec<_>>>()?;
        return Ok(std::env::join_paths(mapped)?
            .to_str()
            .context("non-utf8 path in remote environment rewrite")?
            .to_owned());
    }

    Ok(value.to_owned())
}

fn build_expected_outputs(
    mapper: &mut PathMapper,
    outputs: &[PathBuf],
) -> CargoResult<BTreeMap<String, PathBuf>> {
    let mut expected = BTreeMap::new();
    for local in outputs {
        let remote = mapper.map_output_command_path(local)?;
        if remote == "." {
            bail!("unexpected output path at the remote working directory root");
        }
        expected.insert(remote, local.clone());
    }
    Ok(expected)
}

enum OutputMaterialization<'a> {
    File(&'a repb::OutputFile),
    Symlink(&'a repb::OutputSymlink),
}

fn emit_lines(bytes: &[u8], callback: &mut dyn FnMut(&str) -> CargoResult<()>) -> CargoResult<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    for line in String::from_utf8_lossy(bytes).lines() {
        callback(line)?;
    }
    Ok(())
}

fn rewrite_remap_path_prefix(mapper: &mut PathMapper, value: &str) -> CargoResult<String> {
    let (from, to) = value
        .split_once('=')
        .context("invalid `--remap-path-prefix` argument")?;
    let remote = if mapper.resolve(from).exists() {
        mapper.map_existing_command_path(&mapper.resolve(from))?
    } else {
        from.to_owned()
    };
    Ok(format!("--remap-path-prefix={remote}={to}"))
}

fn rewrite_named_path_value(mapper: &mut PathMapper, value: &str) -> CargoResult<String> {
    let Some((prefix, path)) = value.rsplit_once('=') else {
        return Ok(value.to_owned());
    };
    let local = mapper.resolve(path);
    if !local.exists() {
        return Ok(value.to_owned());
    }
    Ok(format!(
        "{prefix}={}",
        mapper.map_input_command_path(&local)?
    ))
}

fn rewrite_search_path_value(mapper: &mut PathMapper, value: &str) -> CargoResult<String> {
    if let Some((kind, path)) = value.split_once('=') {
        let local = mapper.resolve(path);
        if local.exists() {
            return Ok(format!("{kind}={}", mapper.map_input_command_path(&local)?));
        }
    }
    let local = mapper.resolve(value);
    if local.exists() {
        mapper.map_input_command_path(&local)
    } else {
        Ok(value.to_owned())
    }
}

fn rewrite_codegen_arg(mapper: &mut PathMapper, value: &str) -> CargoResult<String> {
    if let Some(path) = value.strip_prefix("linker=") {
        let local = mapper.resolve(path);
        if local.exists() {
            return Ok(format!("linker={}", mapper.map_input_command_path(&local)?));
        }
    }
    if let Some(path) = value.strip_prefix("link-arg=") {
        let local = mapper.resolve(path);
        if local.exists() {
            return Ok(format!(
                "link-arg={}",
                mapper.map_input_command_path(&local)?
            ));
        }
    }
    Ok(value.to_owned())
}

fn maybe_existing_path(mapper: &PathMapper, value: &str) -> Option<PathBuf> {
    let path = mapper.resolve(value);
    path.exists().then_some(path)
}

struct PathMapper {
    cwd: PathBuf,
    target_dir: PathBuf,
    build_dir: PathBuf,
    sysroot: PathBuf,
    output_paths: BTreeSet<PathBuf>,
    external_roots: Vec<(PathBuf, String)>,
    tree: DirBuilder,
    uploads: UploadMap,
}

impl PathMapper {
    fn new(
        cwd: PathBuf,
        target_dir: PathBuf,
        build_dir: PathBuf,
        sysroot: PathBuf,
        output_paths: BTreeSet<PathBuf>,
    ) -> CargoResult<Self> {
        let mut tree = DirBuilder::default();
        tree.ensure_dir(&[WORKDIR_NAME.to_owned()]);
        Ok(Self {
            cwd,
            target_dir,
            build_dir,
            sysroot,
            output_paths,
            external_roots: Vec::new(),
            tree,
            uploads: BTreeMap::new(),
        })
    }

    fn root_digest(&mut self) -> CargoResult<repb::Digest> {
        self.tree.finalize(&mut self.uploads)
    }

    fn resolve(&self, value: &str) -> PathBuf {
        let path = PathBuf::from(value);
        if path.is_absolute() {
            path
        } else {
            self.cwd.join(path)
        }
    }

    fn add_package_tree(&mut self, root: &Path) -> CargoResult<()> {
        let remote = self.execroot_path(root)?;
        self.add_local_path(root, &remote, true)
    }

    fn add_input_path(&mut self, path: &Path) -> CargoResult<()> {
        let remote = self.execroot_path(path)?;
        self.add_local_path(path, &remote, false)
    }

    fn add_blob(&mut self, digest: repb::Digest, data: UploadData) {
        self.uploads
            .entry(DigestKey::from_digest(&digest))
            .or_insert_with(|| UploadBlob { digest, data });
    }

    fn map_input_command_path(&mut self, path: &Path) -> CargoResult<String> {
        self.add_input_path(path)?;
        self.map_existing_command_path(path)
    }

    fn map_output_command_path(&mut self, path: &Path) -> CargoResult<String> {
        let execroot = self.execroot_path(path)?;
        Ok(to_command_path(&execroot))
    }

    fn map_existing_command_path(&mut self, path: &Path) -> CargoResult<String> {
        let execroot = self.execroot_path(path)?;
        Ok(to_command_path(&execroot))
    }

    fn add_local_path(
        &mut self,
        local: &Path,
        remote_execroot: &str,
        exclude_build_roots: bool,
    ) -> CargoResult<()> {
        if self.output_paths.contains(local) {
            return Ok(());
        }
        if exclude_build_roots
            && (local.starts_with(&self.target_dir) || local.starts_with(&self.build_dir))
        {
            return Ok(());
        }
        if local.file_name().is_some_and(|name| name == ".git") {
            return Ok(());
        }

        let metadata = fs::symlink_metadata(local)
            .with_context(|| format!("failed to read metadata for `{}`", local.display()))?;
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(local)
                .with_context(|| format!("failed to read symlink `{}`", local.display()))?;
            self.tree
                .insert_symlink(remote_execroot, to_reapi_path(&target))?;
            return Ok(());
        }

        if metadata.is_dir() {
            self.tree.ensure_dir(&path_components(remote_execroot));
            for entry in fs::read_dir(local)? {
                let entry = entry?;
                let child = entry.path();
                let child_remote = join_execroot_path(remote_execroot, &entry.file_name());
                self.add_local_path(&child, &child_remote, exclude_build_roots)?;
            }
            return Ok(());
        }

        let (digest, data) = digest_file(local)?;
        self.add_blob(digest.clone(), data);
        self.tree.insert_file(
            remote_execroot,
            FileEntry {
                digest,
                is_executable: is_executable(&metadata),
            },
        )?;
        Ok(())
    }

    fn execroot_path(&mut self, path: &Path) -> CargoResult<String> {
        if let Ok(rel) = path.strip_prefix(&self.cwd) {
            return Ok(join_remote(WORKDIR_NAME, rel));
        }
        if let Ok(rel) = path.strip_prefix(&self.target_dir) {
            return Ok(join_remote(
                &format!("{WORKDIR_NAME}/{AUX_ROOT}/target-dir"),
                rel,
            ));
        }
        if let Ok(rel) = path.strip_prefix(&self.build_dir) {
            return Ok(join_remote(
                &format!("{WORKDIR_NAME}/{AUX_ROOT}/build-dir"),
                rel,
            ));
        }
        if let Ok(rel) = path.strip_prefix(&self.sysroot) {
            return Ok(join_remote(
                &format!("{WORKDIR_NAME}/{AUX_ROOT}/toolchain"),
                rel,
            ));
        }
        for (root, slot) in &self.external_roots {
            if let Ok(rel) = path.strip_prefix(root) {
                return Ok(join_remote(slot, rel));
            }
        }

        let root = if path.is_dir() {
            path.to_path_buf()
        } else {
            path.parent().unwrap_or(path).to_path_buf()
        };
        let slot = format!(
            "{WORKDIR_NAME}/{AUX_ROOT}/external/{}",
            short_digest_of_path(&root)
        );
        self.external_roots.push((root.clone(), slot.clone()));
        if let Ok(rel) = path.strip_prefix(&root) {
            return Ok(join_remote(&slot, rel));
        }
        Ok(slot)
    }
}

#[derive(Default)]
struct DirBuilder {
    files: BTreeMap<String, FileEntry>,
    dirs: BTreeMap<String, DirBuilder>,
    symlinks: BTreeMap<String, String>,
}

impl DirBuilder {
    fn ensure_dir(&mut self, components: &[String]) {
        let mut dir = self;
        for component in components {
            dir = dir.dirs.entry(component.clone()).or_default();
        }
    }

    fn insert_file(&mut self, remote_execroot: &str, entry: FileEntry) -> CargoResult<()> {
        let components = path_components(remote_execroot);
        let (file, dirs) = components
            .split_last()
            .context("remote file path must contain a file name")?;
        let mut dir = self;
        for component in dirs {
            dir = dir.dirs.entry(component.clone()).or_default();
        }
        dir.files.insert(file.clone(), entry);
        Ok(())
    }

    fn insert_symlink(&mut self, remote_execroot: &str, target: String) -> CargoResult<()> {
        let components = path_components(remote_execroot);
        let (name, dirs) = components
            .split_last()
            .context("remote symlink path must contain a file name")?;
        let mut dir = self;
        for component in dirs {
            dir = dir.dirs.entry(component.clone()).or_default();
        }
        dir.symlinks.insert(name.clone(), target);
        Ok(())
    }

    fn finalize(&self, uploads: &mut UploadMap) -> CargoResult<repb::Digest> {
        let directories = self
            .dirs
            .iter()
            .map(|(name, dir)| {
                let digest = dir.finalize(uploads)?;
                Ok(repb::DirectoryNode {
                    name: name.clone(),
                    digest: Some(digest),
                })
            })
            .collect::<CargoResult<Vec<_>>>()?;
        let files = self
            .files
            .iter()
            .map(|(name, file)| repb::FileNode {
                name: name.clone(),
                digest: Some(file.digest.clone()),
                is_executable: file.is_executable,
            })
            .collect();
        let symlinks = self
            .symlinks
            .iter()
            .map(|(name, target)| repb::SymlinkNode {
                name: name.clone(),
                target: target.clone(),
            })
            .collect();

        let directory = repb::Directory {
            files,
            directories,
            symlinks,
        };
        let bytes = directory.encode_to_vec();
        let digest = digest_bytes(&bytes);
        uploads
            .entry(DigestKey::from_digest(&digest))
            .or_insert_with(|| UploadBlob {
                digest: digest.clone(),
                data: UploadData::Bytes(bytes),
            });
        Ok(digest)
    }
}

struct FileEntry {
    digest: repb::Digest,
    is_executable: bool,
}

type UploadMap = BTreeMap<DigestKey, UploadBlob>;

struct UploadBlob {
    digest: repb::Digest,
    data: UploadData,
}

impl UploadBlob {
    fn read_bytes(&self) -> CargoResult<Vec<u8>> {
        self.data.read_bytes()
    }
}

enum UploadData {
    Bytes(Vec<u8>),
    File(PathBuf),
}

impl UploadData {
    fn read_bytes(&self) -> CargoResult<Vec<u8>> {
        match self {
            UploadData::Bytes(bytes) => Ok(bytes.clone()),
            UploadData::File(path) => Ok(fs::read(path)?),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DigestKey {
    hash: String,
    size_bytes: i64,
}

impl DigestKey {
    fn from_digest(digest: &repb::Digest) -> Self {
        Self {
            hash: digest.hash.clone(),
            size_bytes: digest.size_bytes,
        }
    }
}

fn platform_from_config(config: &RemoteBuildConfig) -> repb::Platform {
    let mut properties = BTreeMap::from([
        ("Arch".to_owned(), default_arch().to_owned()),
        ("OSFamily".to_owned(), default_os_family().to_owned()),
    ]);
    for (name, value) in &config.exec_properties {
        properties.insert(name.clone(), value.clone());
    }
    repb::Platform {
        properties: properties
            .into_iter()
            .map(|(name, value)| repb::platform::Property { name, value })
            .collect(),
    }
}

fn default_os_family() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "linux" => "linux",
        "windows" => "windows",
        other => other,
    }
}

fn default_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn digest_file(path: &Path) -> CargoResult<(repb::Digest, UploadData)> {
    let metadata = fs::metadata(path)?;
    if metadata.len() <= INLINE_BLOB_LIMIT {
        let bytes = fs::read(path)?;
        return Ok((digest_bytes(&bytes), UploadData::Bytes(bytes)));
    }

    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = repb::Digest {
        hash: hex::encode(hasher.finalize()),
        size_bytes: metadata.len() as i64,
    };
    Ok((digest, UploadData::File(path.to_path_buf())))
}

fn digest_bytes(bytes: &[u8]) -> repb::Digest {
    repb::Digest {
        hash: hex::encode(Sha256::digest(bytes)),
        size_bytes: bytes.len() as i64,
    }
}

fn join_remote(base: &str, rel: &Path) -> String {
    if rel.as_os_str().is_empty() {
        base.to_owned()
    } else {
        format!("{base}/{}", to_reapi_path(rel))
    }
}

fn join_execroot_path(base: &str, child: &OsString) -> String {
    if base.is_empty() {
        to_reapi_path(Path::new(child))
    } else {
        format!("{base}/{}", to_reapi_path(Path::new(child)))
    }
}

fn to_command_path(execroot_path: &str) -> String {
    if execroot_path == WORKDIR_NAME {
        ".".to_owned()
    } else {
        execroot_path
            .strip_prefix(&format!("{WORKDIR_NAME}/"))
            .unwrap_or(execroot_path)
            .to_owned()
    }
}

fn to_reapi_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            std::path::Component::CurDir => Some(".".to_owned()),
            std::path::Component::ParentDir => Some("..".to_owned()),
            std::path::Component::RootDir | std::path::Component::Prefix(_) => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn path_components(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|component| !component.is_empty())
        .map(str::to_owned)
        .collect()
}

fn short_digest_of_path(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_os_str().as_encoded_bytes());
    hex::encode(hasher.finalize())[..16].to_owned()
}

fn upload_resource_name(instance_name: &str, digest: &repb::Digest) -> String {
    let upload_id = random_uuid_v4();
    if instance_name.is_empty() {
        format!(
            "uploads/{upload_id}/blobs/sha256/{}/{}",
            digest.hash, digest.size_bytes
        )
    } else {
        format!(
            "{instance_name}/uploads/{upload_id}/blobs/sha256/{}/{}",
            digest.hash, digest.size_bytes
        )
    }
}

fn random_uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
        u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
        u16::from_be_bytes(bytes[8..10].try_into().unwrap()),
        u64::from_be_bytes([0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]])
    )
}

fn download_resource_name(instance_name: &str, digest: &repb::Digest) -> String {
    if instance_name.is_empty() {
        format!("blobs/sha256/{}/{}", digest.hash, digest.size_bytes)
    } else {
        format!(
            "{instance_name}/blobs/sha256/{}/{}",
            digest.hash, digest.size_bytes
        )
    }
}

fn resolve_program_path(cmd: &ProcessBuilder, program: &Path) -> CargoResult<PathBuf> {
    if program.components().count() > 1 {
        return Ok(program.to_path_buf());
    }

    let path_env = cmd
        .get_env("PATH")
        .or_else(|| std::env::var_os("PATH"))
        .context("remote execution requires a PATH to resolve tool executables")?;
    let candidates = std::env::split_paths(&path_env).flat_map(|path| {
        let candidate = path.join(program);
        let with_exe = if std::env::consts::EXE_EXTENSION.is_empty() {
            None
        } else {
            Some(candidate.with_extension(std::env::consts::EXE_EXTENSION))
        };
        std::iter::once(candidate).chain(with_exe)
    });

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    bail!("no executable for `{}` found in PATH", program.display())
}

fn utf8_args(cmd: &ProcessBuilder) -> CargoResult<Vec<String>> {
    cmd.get_args().map(os_to_utf8).collect()
}

fn os_to_utf8(value: &OsString) -> CargoResult<String> {
    value
        .to_str()
        .map(str::to_owned)
        .with_context(|| format!("remote execution requires UTF-8, found `{value:?}`"))
}

fn unpack_execute_response(any: prost_types::Any) -> CargoResult<repb::ExecuteResponse> {
    repb::ExecuteResponse::decode(any.value.as_slice())
        .context("failed to decode ExecuteResponse from operation payload")
}

fn remove_existing_output(path: &Path) -> CargoResult<()> {
    if !path.try_exists()? {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        paths::remove_dir_all(path)?;
    } else {
        paths::remove_file(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> CargoResult<()> {
    use std::os::unix::fs::PermissionsExt;

    if !executable {
        return Ok(());
    }
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(permissions.mode() | 0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> CargoResult<()> {
    Ok(())
}

#[cfg(unix)]
fn write_symlink(path: &Path, target: &str) -> CargoResult<()> {
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(windows)]
fn write_symlink(path: &Path, target: &str) -> CargoResult<()> {
    let target_path = Path::new(target);
    if target_path.ends_with(std::path::MAIN_SEPARATOR_STR) || target_path.is_dir() {
        std::os::windows::fs::symlink_dir(target_path, path)?;
    } else {
        std::os::windows::fs::symlink_file(target_path, path)?;
    }
    Ok(())
}

fn is_executable(metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    fn digest(hash: &str, size_bytes: i64) -> repb::Digest {
        repb::Digest {
            hash: hash.to_owned(),
            size_bytes,
        }
    }

    #[test]
    fn resource_names_include_instance_name() {
        let digest = digest("abc123", 42);

        let upload = upload_resource_name("buildbuddy", &digest);
        assert!(upload.starts_with("buildbuddy/uploads/"));
        assert!(upload.ends_with("/blobs/sha256/abc123/42"));

        let download = download_resource_name("buildbuddy", &digest);
        assert_eq!(download, "buildbuddy/blobs/sha256/abc123/42");
    }

    #[test]
    fn dir_builder_inserts_nested_files_by_leaf_name() {
        let mut builder = DirBuilder::default();
        builder
            .insert_file(
                "workspace/crate/target.rmeta",
                FileEntry {
                    digest: digest("deadbeef", 9),
                    is_executable: false,
                },
            )
            .unwrap();

        assert!(
            builder
                .dirs
                .get("workspace")
                .and_then(|dir| dir.dirs.get("crate"))
                .is_some_and(|dir| dir.files.contains_key("target.rmeta"))
        );
    }

    #[test]
    fn external_paths_share_the_same_execroot_slot() {
        let temp = tempdir().unwrap();
        let cwd = temp.path().join("cwd");
        let target_dir = temp.path().join("target");
        let build_dir = temp.path().join("build");
        let sysroot = temp.path().join("sysroot");
        let external_root = temp.path().join("external/dep");

        for path in [&cwd, &target_dir, &build_dir, &sysroot, &external_root] {
            fs::create_dir_all(path).unwrap();
        }

        let mut mapper =
            PathMapper::new(cwd, target_dir, build_dir, sysroot, BTreeSet::new()).unwrap();
        let first = mapper.execroot_path(&external_root.join("one.rs")).unwrap();
        let second = mapper.execroot_path(&external_root.join("two.rs")).unwrap();

        assert!(first.starts_with(&format!("{WORKDIR_NAME}/{AUX_ROOT}/external/")));
        assert_eq!(
            first.rsplit_once('/').map(|(prefix, _)| prefix),
            second.rsplit_once('/').map(|(prefix, _)| prefix)
        );
    }
}
