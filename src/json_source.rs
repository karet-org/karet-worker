//! JSON source reader: NDJSON / JSON-array files into a [`DataFrame`].
//!
//! Schema columns bind to paths inside each record (`request.headers.X[0]`)
//! rather than to CSV headers, so nested logs need no pre-flattening. An
//! unresolvable path yields null, which the column assertions can catch;
//! mixed-shape records are the norm in log streams and must not abort a file.

use polars::prelude::*;
use serde_json::Value;

use crate::config::{ColumnSchema, SourceContainer, SourceFormat};

/// One step of a compiled path: a map key or an array index.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    Key(String),
    Index(usize),
}

/// Compile `a.b[0].c` into steps. Keys may contain any character except
/// `.` and `[`; an unterminated or non-numeric index is an error.
fn compile_path(path: &str) -> Result<Vec<Step>, String> {
    let mut steps = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                if !cur.is_empty() {
                    steps.push(Step::Key(std::mem::take(&mut cur)));
                }
            }
            '[' => {
                if !cur.is_empty() {
                    steps.push(Step::Key(std::mem::take(&mut cur)));
                }
                let mut digits = String::new();
                loop {
                    match chars.next() {
                        Some(']') => break,
                        Some(d) if d.is_ascii_digit() => digits.push(d),
                        _ => return Err(format!("malformed array index in path '{path}'")),
                    }
                }
                let idx: usize = digits
                    .parse()
                    .map_err(|_| format!("malformed array index in path '{path}'"))?;
                steps.push(Step::Index(idx));
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        steps.push(Step::Key(cur));
    }
    if steps.is_empty() {
        return Err(format!("empty path '{path}'"));
    }
    Ok(steps)
}

/// Resolve compiled steps against a record. Missing keys, out-of-range
/// indices and type mismatches all resolve to `None`.
fn resolve<'a>(record: &'a Value, steps: &[Step]) -> Option<&'a Value> {
    let mut cur = record;
    for step in steps {
        cur = match step {
            Step::Key(k) => cur.as_object()?.get(k)?,
            Step::Index(i) => cur.as_array()?.get(*i)?,
        };
    }
    Some(cur)
}

/// Column builder: one typed vector per schema column.
enum Builder {
    Str(Vec<Option<String>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
}

impl Builder {
    fn for_type(type_: &str, capacity: usize) -> Self {
        match type_ {
            "int64" => Builder::I64(Vec::with_capacity(capacity)),
            "number" | "float64" => Builder::F64(Vec::with_capacity(capacity)),
            "bool" => Builder::Bool(Vec::with_capacity(capacity)),
            // "string", "date" and anything unrecognized land as strings;
            // mapping expressions (parse_date, cast, from_unix) convert.
            _ => Builder::Str(Vec::with_capacity(capacity)),
        }
    }

    /// Append one JSON value, coercing when lossless and nulling otherwise.
    fn push(&mut self, value: Option<&Value>) {
        match self {
            Builder::Str(v) => v.push(match value {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Number(n)) => Some(n.to_string()),
                Some(Value::Bool(b)) => Some(b.to_string()),
                _ => None,
            }),
            Builder::I64(v) => v.push(match value {
                Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
                Some(Value::String(s)) => s.parse().ok(),
                _ => None,
            }),
            Builder::F64(v) => v.push(match value {
                Some(Value::Number(n)) => n.as_f64(),
                Some(Value::String(s)) => s.parse().ok(),
                _ => None,
            }),
            Builder::Bool(v) => v.push(match value {
                Some(Value::Bool(b)) => Some(*b),
                _ => None,
            }),
        }
    }

    fn finish(self, name: &str) -> Column {
        match self {
            Builder::Str(v) => Series::new(name.into(), v).into(),
            Builder::I64(v) => Series::new(name.into(), v).into(),
            Builder::F64(v) => Series::new(name.into(), v).into(),
            Builder::Bool(v) => Series::new(name.into(), v).into(),
        }
    }
}

/// Split file bytes into records according to the format. Unparseable
/// NDJSON lines are skipped (count returned) rather than failing the file.
fn parse_records(bytes: &[u8], format: SourceFormat) -> Result<(Vec<Value>, usize), String> {
    match format {
        SourceFormat::JsonArray => {
            let parsed: Value =
                serde_json::from_slice(bytes).map_err(|e| format!("invalid JSON: {e}"))?;
            match parsed {
                Value::Array(items) => Ok((items, 0)),
                other => Ok((vec![other], 0)),
            }
        }
        SourceFormat::Ndjson => {
            let text = std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {e}"))?;
            let mut records = Vec::new();
            let mut skipped = 0usize;
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(v) => records.push(v),
                    Err(_) => skipped += 1,
                }
            }
            Ok((records, skipped))
        }
        SourceFormat::Csv => Err("read_json called for a CSV source".to_string()),
    }
}

/// Read a JSON source file into a [`DataFrame`] with one column per schema
/// entry, typed per the declared logical type. Returns the frame plus the
/// number of unparseable records skipped.
pub fn read_json_source(
    bytes: &[u8],
    source: &SourceContainer,
) -> Result<(DataFrame, usize), String> {
    let (records, skipped) = parse_records(bytes, source.format)?;

    // Compile paths once per file, not per record.
    let mut compiled: Vec<(&ColumnSchema, Vec<Step>)> = Vec::with_capacity(source.schema.len());
    for col in &source.schema {
        let path = col.path.as_deref().unwrap_or(&col.name);
        compiled.push((col, compile_path(path)?));
    }

    let mut builders: Vec<Builder> = source
        .schema
        .iter()
        .map(|c| Builder::for_type(&c.type_, records.len()))
        .collect();

    for record in &records {
        for (i, (_, steps)) in compiled.iter().enumerate() {
            builders[i].push(resolve(record, steps));
        }
    }

    let columns: Vec<Column> = builders
        .into_iter()
        .zip(source.schema.iter())
        .map(|(b, c)| b.finish(&c.name))
        .collect();

    let height = records.len();
    let df = DataFrame::new(height, columns).map_err(|e| format!("frame build failed: {e}"))?;
    Ok((df, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnSchema;

    fn col(name: &str, type_: &str, path: Option<&str>) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            type_: type_.into(),
            nullable: None,
            assertions: None,
            path: path.map(|p| p.to_string()),
        }
    }

    fn source(format: SourceFormat, schema: Vec<ColumnSchema>) -> SourceContainer {
        SourceContainer {
            id: "s".into(),
            name: "S".into(),
            path_prefix: "logs/".into(),
            format,
            schema,
            record_filter: None,
        }
    }

    #[test]
    fn compiles_dotted_and_indexed_paths() {
        assert_eq!(
            compile_path("request.headers.User-Agent[0]").unwrap(),
            vec![
                Step::Key("request".into()),
                Step::Key("headers".into()),
                Step::Key("User-Agent".into()),
                Step::Index(0),
            ]
        );
        assert!(compile_path("a[x]").is_err());
        assert!(compile_path("").is_err());
    }

    #[test]
    fn extracts_nested_caddy_shaped_record() {
        let line = br#"{"ts":1788934940.86,"status":200,"request":{"host":"etl.example","headers":{"User-Agent":["curl/8"],"Cf-Ipcountry":["CA"]}}}"#;
        let src = source(
            SourceFormat::Ndjson,
            vec![
                col("ts", "number", None),
                col("status", "int64", None),
                col("host", "string", Some("request.host")),
                col("ua", "string", Some("request.headers.User-Agent[0]")),
                col("country", "string", Some("request.headers.Cf-Ipcountry[0]")),
            ],
        );
        let (df, skipped) = read_json_source(line, &src).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("host").unwrap().as_materialized_series().str().unwrap().get(0),
            Some("etl.example")
        );
        assert_eq!(
            df.column("ua").unwrap().as_materialized_series().str().unwrap().get(0),
            Some("curl/8")
        );
        assert_eq!(
            df.column("status").unwrap().as_materialized_series().i64().unwrap().get(0),
            Some(200)
        );
        let ts = df.column("ts").unwrap().as_materialized_series().f64().unwrap().get(0).unwrap();
        assert!((ts - 1788934940.86).abs() < 1e-6);
    }

    #[test]
    fn missing_paths_and_shape_mismatches_become_null() {
        let lines = br#"{"a":1}
{"a":2,"nested":{"b":"x"}}"#;
        let src = source(
            SourceFormat::Ndjson,
            vec![col("a", "int64", None), col("b", "string", Some("nested.b"))],
        );
        let (df, _) = read_json_source(lines, &src).unwrap();
        assert_eq!(df.height(), 2);
        let b = df.column("b").unwrap().as_materialized_series().str().unwrap();
        assert_eq!(b.get(0), None, "absent path is null, not an error");
        assert_eq!(b.get(1), Some("x"));
    }

    #[test]
    fn skips_unparseable_ndjson_lines() {
        let lines = b"{\"a\":1}\nnot json at all\n\n{\"a\":3}\n";
        let src = source(SourceFormat::Ndjson, vec![col("a", "int64", None)]);
        let (df, skipped) = read_json_source(lines, &src).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn reads_a_json_array_file() {
        let body = br#"[{"a":1},{"a":2},{"a":3}]"#;
        let src = source(SourceFormat::JsonArray, vec![col("a", "int64", None)]);
        let (df, _) = read_json_source(body, &src).unwrap();
        assert_eq!(df.height(), 3);
    }

    #[test]
    fn coerces_across_json_types_when_lossless() {
        let line = br#"{"n":"42","s":7,"b":true}"#;
        let src = source(
            SourceFormat::Ndjson,
            vec![col("n", "int64", None), col("s", "string", None), col("b", "bool", None)],
        );
        let (df, _) = read_json_source(line, &src).unwrap();
        assert_eq!(df.column("n").unwrap().as_materialized_series().i64().unwrap().get(0), Some(42));
        assert_eq!(df.column("s").unwrap().as_materialized_series().str().unwrap().get(0), Some("7"));
        assert_eq!(df.column("b").unwrap().as_materialized_series().bool().unwrap().get(0), Some(true));
    }
}
