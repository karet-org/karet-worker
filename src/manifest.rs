//! Table manifests: the warehouse's publish point.
//!
//! A run writes Parquet under `v<version>/`, never over the objects a reader
//! might be scanning, then publishes by overwriting one small object:
//!
//! ```text
//! pipelines/<slug>/<table>/_current.json        {"version": 7}
//! pipelines/<slug>/<table>/_manifests/7.json    the file list for version 7
//! pipelines/<slug>/<table>/v7/year=2026/month=9/<mapping>.parquet
//! ```
//!
//! Readers resolve `_current.json`, then the manifest, then scan exactly the
//! files it lists. Since a single-object PUT is atomic, a reader sees either
//! the whole previous version or the whole new one, and a run that dies
//! part-way leaves orphaned objects that no manifest references rather than a
//! half-written table.
//!
//! Keeping the last few manifests is what makes rollback and time travel
//! possible; [`vacuum_targets`] bounds what that costs.

use serde::{Deserialize, Serialize};

/// Pointer object naming the version readers should scan.
pub const CURRENT_KEY: &str = "_current.json";
/// Directory of manifests, keyed by version.
pub const MANIFEST_PREFIX: &str = "_manifests/";
/// How many versions stay readable (and rollback-able) behind the current one.
pub const RETAINED_VERSIONS: usize = 10;

/// The `_current.json` pointer.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentPointer {
    pub version: u64,
}

/// One Parquet object in a table version.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ManifestFile {
    /// Key relative to the table prefix, e.g. `v7/year=2026/month=9/m.parquet`.
    /// Relative so a pipeline can be copied or renamed without rewriting it.
    pub key: String,
    /// Which mapping produced it; a mapping replaces its own files wholesale.
    pub mapping_id: String,
    pub bytes: u64,
}

/// The set of files that make up one version of a table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct TableManifest {
    pub version: u64,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub files: Vec<ManifestFile>,
}

impl TableManifest {
    /// An empty version 0: what a table that has never been written looks like.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Total bytes across the version.
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }
}

/// Prefix a run's outputs live under.
pub fn version_prefix(version: u64) -> String {
    format!("v{version}/")
}

/// Key of a version's manifest, relative to the table prefix.
pub fn manifest_key(version: u64) -> String {
    format!("{MANIFEST_PREFIX}{version}.json")
}

/// Replace `mapping_id`'s contribution to a file list, keeping every other
/// mapping's files untouched.
///
/// A mapping's output is complete on every run (each run re-reads the whole
/// source prefix), so its previous files are always superseded: that is what
/// makes a partition that no longer exists in the source disappear from the
/// table. Other mappings writing the same table (a union) are carried forward
/// so one mapping's run doesn't drop another's rows, and a mapping that failed
/// this run keeps the files it published last time.
pub fn apply_mapping_writes(
    previous: &[ManifestFile],
    mapping_id: &str,
    files: Vec<ManifestFile>,
) -> Vec<ManifestFile> {
    let mut out: Vec<ManifestFile> = previous
        .iter()
        .filter(|f| f.mapping_id != mapping_id)
        .cloned()
        .collect();
    out.extend(files);
    out.sort_by(|a, b| a.key.cmp(&b.key));
    out
}

/// Versions to delete, oldest first: everything below the retention window.
pub fn versions_to_drop(mut versions: Vec<u64>, current: u64, retain: usize) -> Vec<u64> {
    versions.sort_unstable();
    versions.retain(|v| *v <= current);
    let keep_from = versions.len().saturating_sub(retain);
    versions[..keep_from].to_vec()
}

/// Objects safe to delete: everything under the table prefix that no retained
/// manifest references, excluding the manifests and pointer themselves.
///
/// Driven off the retained manifests rather than off version numbers, so a
/// partially written run's orphans get collected too.
pub fn unreferenced_keys(retained: &[TableManifest], present: &[String]) -> Vec<String> {
    let referenced: std::collections::HashSet<&str> = retained
        .iter()
        .flat_map(|m| m.files.iter().map(|f| f.key.as_str()))
        .collect();
    present
        .iter()
        .filter(|k| {
            !k.starts_with(MANIFEST_PREFIX) && *k != CURRENT_KEY && !referenced.contains(k.as_str())
        })
        .cloned()
        .collect()
}

/// Read the current manifest for a table, or an empty version 0 when the table
/// has never been published.
pub async fn read_current(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
) -> Result<TableManifest, String> {
    let pointer = match crate::s3::get_bytes(client, bucket, &format!("{table_prefix}{CURRENT_KEY}")).await
    {
        Ok(bytes) => serde_json::from_slice::<CurrentPointer>(&bytes)
            .map_err(|e| format!("{table_prefix}{CURRENT_KEY} is not a version pointer: {e}"))?,
        // Absent pointer means an unpublished table, not a failure.
        Err(_) => return Ok(TableManifest::empty()),
    };
    read_version(client, bucket, table_prefix, pointer.version).await
}

/// Read one version's manifest.
pub async fn read_version(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
    version: u64,
) -> Result<TableManifest, String> {
    let key = format!("{table_prefix}{}", manifest_key(version));
    let bytes = crate::s3::get_bytes(client, bucket, &key).await?;
    serde_json::from_slice(&bytes).map_err(|e| format!("{key} is not a manifest: {e}"))
}

/// Publish `files` as the table's next version: write the manifest, then flip
/// the pointer. The pointer PUT is the moment readers switch over.
pub async fn publish(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
    manifest: &TableManifest,
) -> Result<(), String> {
    let put = |key: String, body: Vec<u8>| async move {
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(body))
            .content_type("application/json")
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("PutObject {key}: {}", crate::s3::err_chain(&e)))
    };
    let body = serde_json::to_vec(manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    put(format!("{table_prefix}{}", manifest_key(manifest.version)), body).await?;
    let pointer = serde_json::to_vec(&CurrentPointer { version: manifest.version })
        .map_err(|e| format!("serialize pointer: {e}"))?;
    put(format!("{table_prefix}{CURRENT_KEY}"), pointer).await
}

/// Delete manifests outside the retention window and any object no retained
/// manifest references.
///
/// Runs after the pointer flip, so a reader that resolved a manifest more than
/// [`RETAINED_VERSIONS`] runs ago can have files deleted from under it. That is
/// the same trade Delta's `VACUUM` makes, and the window is generous next to
/// how long a dashboard query lives.
pub async fn vacuum(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    table_prefix: &str,
    current: u64,
) -> Result<usize, String> {
    let all = crate::s3::list_keys(client, bucket, table_prefix).await?;
    let relative: Vec<String> = all
        .iter()
        .filter_map(|k| k.strip_prefix(table_prefix).map(str::to_string))
        .collect();

    let versions: Vec<u64> = relative
        .iter()
        .filter_map(|k| k.strip_prefix(MANIFEST_PREFIX))
        .filter_map(|v| v.strip_suffix(".json"))
        .filter_map(|v| v.parse().ok())
        .collect();
    let doomed = versions_to_drop(versions.clone(), current, RETAINED_VERSIONS);
    let retained_versions: Vec<u64> =
        versions.into_iter().filter(|v| !doomed.contains(v) && *v <= current).collect();

    let mut retained = Vec::new();
    for v in &retained_versions {
        match read_version(client, bucket, table_prefix, *v).await {
            Ok(m) => retained.push(m),
            // An unreadable manifest is treated as still-live: better to keep
            // orphans than to delete files something might reference.
            Err(e) => return Err(format!("vacuum aborted, manifest {v} unreadable: {e}")),
        }
    }

    let mut deleted = 0usize;
    let mut targets = unreferenced_keys(&retained, &relative);
    targets.extend(doomed.iter().map(|v| manifest_key(*v)));
    for key in targets {
        let full = format!("{table_prefix}{key}");
        match client.delete_object().bucket(bucket).key(&full).send().await {
            Ok(_) => deleted += 1,
            Err(e) => tracing::warn!("vacuum: delete failed for {full}: {}", crate::s3::err_chain(&e)),
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(key: &str, mapping: &str) -> ManifestFile {
        ManifestFile { key: key.into(), mapping_id: mapping.into(), bytes: 10 }
    }

    fn manifest(version: u64, files: Vec<ManifestFile>) -> TableManifest {
        TableManifest { version, created_at: "2026-09-16T00:00:00Z".into(), job_id: None, files }
    }

    #[test]
    fn a_mapping_replaces_only_its_own_files() {
        let prev = manifest(
            1,
            vec![
                file("v1/year=2026/a.parquet", "caddy"),
                file("v1/year=2026/b.parquet", "edge"),
            ],
        );
        let next = apply_mapping_writes(&prev.files, "caddy", vec![file("v2/year=2026/a.parquet", "caddy")]);
        assert_eq!(
            next.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            vec!["v1/year=2026/b.parquet", "v2/year=2026/a.parquet"],
            "the union partner's file survives, the mapping's own is replaced"
        );
    }

    #[test]
    fn vanished_partitions_drop_out() {
        let prev = manifest(
            1,
            vec![
                file("v1/month=8/m.parquet", "m"),
                file("v1/month=9/m.parquet", "m"),
            ],
        );
        let next = apply_mapping_writes(&prev.files, "m", vec![file("v2/month=9/m.parquet", "m")]);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].key, "v2/month=9/m.parquet");
    }

    #[test]
    fn writing_a_table_for_the_first_time() {
        let next = apply_mapping_writes(&[], "m", vec![file("v1/m.parquet", "m")]);
        assert_eq!(next.len(), 1);
    }

    #[test]
    fn a_mapping_that_did_not_run_keeps_its_files() {
        let prev = manifest(1, vec![file("v1/a.parquet", "caddy"), file("v1/b.parquet", "edge")]);
        // Only `edge` wrote this run: `caddy` failed, so its file carries over.
        let next = apply_mapping_writes(&prev.files, "edge", vec![file("v2/b.parquet", "edge")]);
        assert_eq!(
            next.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            vec!["v1/a.parquet", "v2/b.parquet"]
        );
    }

    #[test]
    fn retention_keeps_the_newest_versions() {
        let drop = versions_to_drop((1..=12).collect(), 12, 10);
        assert_eq!(drop, vec![1, 2], "12 versions, keep 10, drop the two oldest");
        assert!(versions_to_drop(vec![1, 2, 3], 3, 10).is_empty());
    }

    #[test]
    fn retention_ignores_versions_above_current() {
        // A crashed run can leave a manifest newer than the pointer; it is not
        // a candidate for retirement until it is published.
        let drop = versions_to_drop(vec![1, 2, 3, 99], 3, 2);
        assert_eq!(drop, vec![1]);
    }

    #[test]
    fn vacuum_collects_orphans_but_never_live_files() {
        let retained = vec![manifest(2, vec![file("v2/m.parquet", "m")])];
        let present = vec![
            "v1/m.parquet".to_string(),
            "v2/m.parquet".to_string(),
            "v3/m.parquet".to_string(),
            "_manifests/2.json".to_string(),
            "_current.json".to_string(),
        ];
        let mut targets = unreferenced_keys(&retained, &present);
        targets.sort();
        assert_eq!(
            targets,
            vec!["v1/m.parquet", "v3/m.parquet"],
            "v3 is a dead run's orphan; manifests and the pointer are never touched"
        );
    }

    #[test]
    fn manifest_round_trips() {
        let m = manifest(3, vec![file("v3/m.parquet", "m")]);
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(serde_json::from_str::<TableManifest>(&json).unwrap(), m);
        let p = CurrentPointer { version: 3 };
        assert_eq!(serde_json::from_str::<CurrentPointer>(r#"{"version":3}"#).unwrap(), p);
    }
}
