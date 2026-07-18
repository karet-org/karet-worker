//! S3-backed `PartitionUploader` plus thin helpers for raw reads/lists.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::pipeline::PartitionUploader;

/// Render an error together with its full [`std::error::Error::source`] chain.
///
/// AWS SDK errors render tersely on their own — a failure to connect is just
/// `"dispatch failure"`, which hides the actionable cause. Walking the source
/// chain surfaces the underlying reason (e.g. `connection refused`, a DNS
/// failure, or a TLS error) so the message points at what actually needs
/// fixing, such as an unreachable `AWS_ENDPOINT_URL`.
fn err_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(inner) = source {
        let text = inner.to_string();
        // Skip layers that just re-print their child verbatim, to avoid
        // "dispatch failure: dispatch failure: ..." noise.
        if !out.ends_with(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = inner.source();
    }
    out
}

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
                    .map_err(|e| format!("S3 PutObject failed for {full_key}: {}", err_chain(&e)))?;
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
        let resp = req.send().await.map_err(|e| format!("S3 ListObjects failed: {}", err_chain(&e)))?;
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
        .map_err(|e| format!("S3 GetObject failed for {key}: {}", err_chain(&e)))?;
    let bytes = resp.body.collect().await.map_err(|e| format!("S3 body read: {}", err_chain(&e)))?;
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::err_chain;
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct TestError {
        msg: String,
        source: Option<Box<TestError>>,
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.msg)
        }
    }

    impl Error for TestError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.source.as_ref().map(|b| b.as_ref() as &(dyn Error + 'static))
        }
    }

    fn chain(msgs: &[&str]) -> TestError {
        let mut iter = msgs.iter().rev();
        let last = iter.next().expect("at least one message");
        let mut err = TestError { msg: last.to_string(), source: None };
        for m in iter {
            err = TestError { msg: m.to_string(), source: Some(Box::new(err)) };
        }
        err
    }

    #[test]
    fn surfaces_the_underlying_cause() {
        // What an SDK dispatch failure looks like once unwrapped.
        let e = chain(&[
            "dispatch failure",
            "error trying to connect",
            "tcp connect error",
            "Connection refused (os error 111)",
        ]);
        assert_eq!(
            err_chain(&e),
            "dispatch failure: error trying to connect: tcp connect error: Connection refused (os error 111)",
        );
    }

    #[test]
    fn does_not_repeat_a_layer_that_reprints_its_child() {
        let e = chain(&["dispatch failure", "dispatch failure"]);
        assert_eq!(err_chain(&e), "dispatch failure");
    }

    #[test]
    fn single_error_is_unchanged() {
        let e = chain(&["S3 body read failed"]);
        assert_eq!(err_chain(&e), "S3 body read failed");
    }
}
