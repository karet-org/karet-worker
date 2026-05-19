//! Structured error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("unknown source container for key `{key}`")]
    UnknownSourceContainer { key: String },

    #[error("no mapping found for source container `{source_container_id}`")]
    NoMapping { source_container_id: String },

    #[error("CSV `{key}` missing required columns: {missing:?}")]
    MissingColumns { key: String, missing: Vec<String> },

    #[error("evaluator error for key `{key}`: {source}")]
    Eval {
        key: String,
        #[source]
        source: EvalError,
    },

    #[error("polars error for key `{key}`: {source}")]
    Polars {
        key: String,
        #[source]
        source: polars::prelude::PolarsError,
    },

    #[error("no files succeeded during ingestion")]
    NoFilesSucceeded,

    #[error("unsupported partition granularity: `{got}` (supported: \"month\")")]
    UnsupportedGranularity { got: String },

    #[error("partition upload failed for key `{key}`: {message}")]
    PartitionUploadFailed { key: String, message: String },
}

impl PipelineError {
    pub fn eval(key: &str, source: EvalError) -> Self {
        Self::Eval {
            key: key.to_string(),
            source,
        }
    }

    pub fn polars(key: &str, source: polars::prelude::PolarsError) -> Self {
        Self::Polars {
            key: key.to_string(),
            source,
        }
    }
}

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("unknown lookup id: {id}")]
    UnknownLookup { id: String },

    #[error("polars error: {0}")]
    Polars(#[from] polars::prelude::PolarsError),
}
