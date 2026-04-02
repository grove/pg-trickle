use tokio_postgres::{Client, NoTls};

use crate::cli::ConnectionArgs;
use crate::error::CliError;

/// Establish a connection to PostgreSQL using the resolved connection parameters.
///
/// Resolution order:
/// 1. `--url` / `-U` CLI argument (highest priority)
/// 2. `PGTRICKLE_URL` environment variable
/// 3. Individual `--host`/`--port`/`--dbname`/`--user`/`--password` flags
///    (which also read from PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD)
/// 4. Defaults: localhost:5432/postgres
pub async fn connect(args: &ConnectionArgs) -> Result<Client, CliError> {
    let conn_string = build_connection_string(args);

    let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await?;

    // Spawn the connection task — it runs until the client is dropped.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok(client)
}

pub fn build_connection_string(args: &ConnectionArgs) -> String {
    if let Some(url) = &args.url {
        return url.clone();
    }

    let host = args.host.as_deref().unwrap_or("localhost");
    let port = args.port.unwrap_or(5432);
    let dbname = args.dbname.as_deref().unwrap_or("postgres");
    let user = args.user.as_deref().unwrap_or("postgres");

    let mut conn = format!("host={host} port={port} dbname={dbname} user={user}");

    if let Some(password) = &args.password {
        // Use the key=value format which handles special characters.
        conn.push_str(&format!(" password={password}"));
    }

    conn
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_connection_string() {
        let args = ConnectionArgs {
            url: None,
            host: None,
            port: None,
            dbname: None,
            user: None,
            password: None,
        };
        let s = build_connection_string(&args);
        assert_eq!(s, "host=localhost port=5432 dbname=postgres user=postgres");
    }

    #[test]
    fn test_url_takes_precedence() {
        let args = ConnectionArgs {
            url: Some("postgres://myhost:9999/mydb".to_string()),
            host: Some("other".to_string()),
            port: Some(1234),
            dbname: None,
            user: None,
            password: None,
        };
        let s = build_connection_string(&args);
        assert_eq!(s, "postgres://myhost:9999/mydb");
    }

    #[test]
    fn test_individual_params() {
        let args = ConnectionArgs {
            url: None,
            host: Some("db.example.com".to_string()),
            port: Some(5433),
            dbname: Some("mydb".to_string()),
            user: Some("admin".to_string()),
            password: Some("secret".to_string()),
        };
        let s = build_connection_string(&args);
        assert_eq!(
            s,
            "host=db.example.com port=5433 dbname=mydb user=admin password=secret"
        );
    }
}
