//! Detect uploads to the lake bucket by listing it on an interval.
//!
//! This is for S3 stores that cannot send object-created notifications to
//! `POST /events/s3`. Every `LAKE_POLL_INTERVAL_SECS`, the loop lists each
//! source `path_prefix` from the live pipeline configs and compares the
//! listing with the keys and ETags it saw last time, which are kept in a Redis
//! hash per prefix. A new or replaced object starts the same debounce as a
//! webhook event for every pipeline that reads that prefix. Removed objects
//! are recorded but do not start a run, matching the webhook, which only
//! acts on object-created events.
//!
//! The first scan of a prefix only records what is there. Objects uploaded
//! while the worker was stopped are found by the next scan after it starts,
//! because the saved listing is in Redis and not in memory.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};

use crate::incremental::SourceObject;
use crate::queue::{self, QueueCtx};

/// Redis hash per prefix: object key to ETag.
pub const SEEN_KEY_PREFIX: &str = "karet:lake:seen:";

/// Field written with every saved listing, so an empty prefix that has been
/// scanned is told apart from one that has never been scanned. S3 keys cannot
/// be empty, so it cannot clash with an object.
const SCANNED_FIELD: &str = "";

pub fn seen_key(prefix: &str) -> String {
    format!("{SEEN_KEY_PREFIX}{prefix}")
}

/// What changed under a prefix since the saved listing.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Diff {
    /// New objects and objects whose ETag changed.
    pub uploads: Vec<SourceObject>,
    /// Keys in the saved listing that are no longer in the bucket.
    pub removed: Vec<String>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.uploads.is_empty() && self.removed.is_empty()
    }
}

pub fn diff(seen: &HashMap<String, String>, listing: &[SourceObject]) -> Diff {
    let uploads: Vec<SourceObject> = listing
        .iter()
        .filter(|o| seen.get(&o.key) != Some(&o.etag))
        .cloned()
        .collect();
    let listed: HashSet<&str> = listing.iter().map(|o| o.key.as_str()).collect();
    let mut removed: Vec<String> = seen
        .keys()
        .filter(|k| k.as_str() != SCANNED_FIELD && !listed.contains(k.as_str()))
        .cloned()
        .collect();
    removed.sort();
    Diff { uploads, removed }
}

/// Result of comparing a listing with the saved one.
#[derive(Debug, PartialEq, Eq)]
pub enum Check {
    /// The prefix had no saved listing. This listing has been saved and
    /// nothing should run.
    FirstScan,
    Changed(Diff),
}

/// Compare `listing` with the saved listing for `prefix`. On the first scan,
/// save `listing` and return [`Check::FirstScan`]. Otherwise nothing is
/// written: call [`save`] once the uploads have been handled, so a failure in
/// between is retried on the next scan.
pub async fn check(
    conn: &mut MultiplexedConnection,
    prefix: &str,
    listing: &[SourceObject],
) -> Result<Check, RedisError> {
    let seen: HashMap<String, String> = conn.hgetall(seen_key(prefix)).await?;
    if !seen.contains_key(SCANNED_FIELD) {
        let all = Diff { uploads: listing.to_vec(), removed: Vec::new() };
        save(conn, prefix, &all).await?;
        return Ok(Check::FirstScan);
    }
    Ok(Check::Changed(diff(&seen, listing)))
}

/// Apply `diff` to the saved listing for `prefix`.
pub async fn save(
    conn: &mut MultiplexedConnection,
    prefix: &str,
    diff: &Diff,
) -> Result<(), RedisError> {
    let key = seen_key(prefix);
    let mut pipe = redis::pipe();
    pipe.atomic();
    let mut fields: Vec<(&str, &str)> = vec![(SCANNED_FIELD, "")];
    fields.extend(diff.uploads.iter().map(|o| (o.key.as_str(), o.etag.as_str())));
    for chunk in fields.chunks(1000) {
        pipe.hset_multiple(&key, chunk).ignore();
    }
    for chunk in diff.removed.chunks(1000) {
        pipe.hdel(&key, chunk).ignore();
    }
    pipe.query_async::<()>(conn).await
}

/// Group `(prefix, pipeline)` rows by prefix.
pub fn group_by_prefix(rows: Vec<(String, String)>) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (prefix, slug) in rows {
        out.entry(prefix).or_default().push(slug);
    }
    for slugs in out.values_mut() {
        slugs.sort();
        slugs.dedup();
    }
    out
}

/// List every source prefix every `interval` and debounce a run for each
/// pipeline whose prefix has new or replaced objects.
pub async fn lake_poll_loop(ctx: Arc<QueueCtx>, interval: std::time::Duration) {
    let shutdown = ctx.shutdown.clone();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut conn: Option<MultiplexedConnection> = None;
    loop {
        ticker.tick().await;
        if *shutdown.borrow() {
            return;
        }
        let rows = match crate::db::live_source_prefixes(&ctx.job_ctx.db).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!("lake poll: reading live configs failed: {e}");
                continue;
            }
        };
        let Some(c) = queue::ensure_conn(&ctx.client, &mut conn, "lake poll").await else {
            continue;
        };
        for (prefix, slugs) in group_by_prefix(rows) {
            if *shutdown.borrow() {
                return;
            }
            match poll_prefix(&ctx, c, &prefix, &slugs).await {
                Ok(()) => {}
                Err(PollError::S3(e)) => tracing::warn!("lake poll: {prefix}: {e}"),
                Err(PollError::Redis(e)) => {
                    tracing::warn!("lake poll: {prefix}: redis: {e}");
                    conn = None;
                    break;
                }
            }
        }
    }
}

enum PollError {
    S3(String),
    Redis(RedisError),
}

impl From<RedisError> for PollError {
    fn from(e: RedisError) -> Self {
        PollError::Redis(e)
    }
}

async fn poll_prefix(
    ctx: &QueueCtx,
    conn: &mut MultiplexedConnection,
    prefix: &str,
    slugs: &[String],
) -> Result<(), PollError> {
    let listing =
        crate::s3::list_objects(&ctx.job_ctx.s3_client, &ctx.job_ctx.lake_bucket, prefix)
            .await
            .map_err(PollError::S3)?;
    let diff = match check(conn, prefix, &listing).await? {
        Check::FirstScan => {
            tracing::info!("lake poll: saved first listing of {prefix} ({} objects)", listing.len());
            return Ok(());
        }
        Check::Changed(diff) if diff.is_empty() => return Ok(()),
        Check::Changed(diff) => diff,
    };
    if !diff.uploads.is_empty() {
        for slug in slugs {
            queue::debounce_event(conn, slug).await?;
        }
        tracing::info!(
            "lake poll: {} new or replaced objects under {prefix}; debounced {}",
            diff.uploads.len(),
            slugs.join(", "),
        );
    }
    save(conn, prefix, &diff).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(key: &str, etag: &str) -> SourceObject {
        SourceObject { key: key.into(), etag: etag.into() }
    }

    fn seen(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut m: HashMap<String, String> =
            pairs.iter().map(|(k, e)| (k.to_string(), e.to_string())).collect();
        m.insert(SCANNED_FIELD.into(), String::new());
        m
    }

    #[test]
    fn same_listing_has_no_changes() {
        let d = diff(&seen(&[("a.csv", "1")]), &[obj("a.csv", "1")]);
        assert!(d.is_empty(), "{d:?}");
    }

    #[test]
    fn new_object_is_an_upload() {
        let d = diff(&seen(&[("a.csv", "1")]), &[obj("a.csv", "1"), obj("b.csv", "2")]);
        assert_eq!(d.uploads, vec![obj("b.csv", "2")]);
        assert!(d.removed.is_empty());
    }

    #[test]
    fn changed_etag_is_an_upload() {
        let d = diff(&seen(&[("a.csv", "1")]), &[obj("a.csv", "9")]);
        assert_eq!(d.uploads, vec![obj("a.csv", "9")]);
    }

    #[test]
    fn missing_object_is_removed_and_not_an_upload() {
        let d = diff(&seen(&[("a.csv", "1"), ("b.csv", "2")]), &[obj("a.csv", "1")]);
        assert!(d.uploads.is_empty());
        assert_eq!(d.removed, vec!["b.csv".to_string()]);
    }

    #[test]
    fn scanned_field_is_never_reported_as_removed() {
        let d = diff(&seen(&[]), &[]);
        assert!(d.is_empty(), "{d:?}");
    }

    #[test]
    fn prefixes_group_their_pipelines_once() {
        let g = group_by_prefix(vec![
            ("shared/".into(), "b".into()),
            ("shared/".into(), "a".into()),
            ("shared/".into(), "a".into()),
            ("own/".into(), "c".into()),
        ]);
        assert_eq!(g.get("shared/").unwrap(), &vec!["a".to_string(), "b".to_string()]);
        assert_eq!(g.get("own/").unwrap(), &vec!["c".to_string()]);
    }
}
