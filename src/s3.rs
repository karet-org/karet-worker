//! S3-backed `PartitionUploader` plus thin helpers for raw reads/lists.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::pipeline::PartitionUploader;

pub struct S3PartitionUploader {
    client: Client,
    bucket: String,
    /// Prefix prepended to partition keys, e.g. `pipelines/visa-spending/`.
    prefix: String,
}

impl S3PartitionUploader {
    pub fn new(client: Client, bucket: String, prefix: String) -> Self {
        Self { client, bucket, prefix }
    }
}

impl PartitionUploader for S3PartitionUploader {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), String> {
        let full_key = format!("{}{}", self.prefix, key);
        let body = ByteStream::from(bytes.to_vec());
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&full_key)
                    .body(body)
                    .content_type("application/octet-stream")
                    .send()
                    .await
                    .map_err(|e| format!("S3 PutObject failed for {full_key}: {e}"))?;
                Ok(())
            })
        })
    }
}

pub async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(t) = &token { req = req.continuation_token(t); }
        let resp = req.send().await.map_err(|e| format!("S3 ListObjects failed: {e}"))?;
        for obj in resp.contents() {
            if let Some(k) = obj.key() { keys.push(k.to_string()); }
        }
        match resp.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    Ok(keys)
}

pub async fn get_bytes(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
    let resp = client.get_object().bucket(bucket).key(key).send().await
        .map_err(|e| format!("S3 GetObject failed for {key}: {e}"))?;
    let bytes = resp.body.collect().await.map_err(|e| format!("S3 body read: {e}"))?;
    Ok(bytes.to_vec())
}
