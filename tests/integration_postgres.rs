//! Upload routing against a live Postgres container.
//!
//! Ignored by default because it needs Docker:
//! `cargo test --test integration_postgres -- --ignored`

use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, GenericImage, ImageExt};

/// The columns of the web app's tables that the router reads.
const SCHEMA: &str = "
CREATE TABLE config_versions (id bigserial PRIMARY KEY, pipeline text NOT NULL, config jsonb NOT NULL);
CREATE TABLE pipelines_current (pipeline text PRIMARY KEY, config_version_id bigint NOT NULL);
";

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration_postgres -- --ignored`"]
async fn routing_reads_source_prefixes_from_live_configs() {
    let container = GenericImage::new("postgres", "17-alpine")
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", "test")
        .start()
        .await
        .expect("start postgres container");
    let port = container.get_host_port_ipv4(5432).await.expect("host port");
    let url = format!("postgres://postgres:test@127.0.0.1:{port}/postgres");

    // The image restarts once after initdb, so retry until it accepts queries.
    let mut pool = None;
    for _ in 0..60 {
        if let Ok(p) = karet_worker::db::connect(&url).await {
            if sqlx::query("SELECT 1").execute(&p).await.is_ok() {
                pool = Some(p);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let pool = pool.expect("postgres accepts connections");
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();

    let configs = [
        // Old version of `a`, not live: its prefix must not route.
        ("a", r#"{"source_containers":[{"path_prefix":"old/"}]}"#),
        ("a", r#"{"source_containers":[{"path_prefix":"shared/"},{"path_prefix":"pipelines/a/x/"}]}"#),
        ("b", r#"{"source_containers":[{"path_prefix":"shared/"},{"path_prefix":""}]}"#),
        ("c", r#"{"source_containers":null}"#),
    ];
    let mut ids = Vec::new();
    for (pipeline, config) in configs {
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO config_versions (pipeline, config) VALUES ($1, $2::jsonb) RETURNING id",
        )
        .bind(pipeline)
        .bind(config)
        .fetch_one(&pool)
        .await
        .unwrap();
        ids.push(id);
    }
    for (pipeline, id) in [("a", ids[1]), ("b", ids[2]), ("c", ids[3])] {
        sqlx::query("INSERT INTO pipelines_current VALUES ($1, $2)")
            .bind(pipeline)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }

    let mut entries = karet_worker::db::live_source_prefixes(&pool).await.unwrap();
    entries.sort();
    assert_eq!(
        entries,
        vec![
            ("pipelines/a/x/".to_string(), "a".to_string()),
            ("shared/".to_string(), "a".to_string()),
            ("shared/".to_string(), "b".to_string()),
        ],
        "live prefixes only; empty prefixes and a non-array source list are skipped"
    );
}
