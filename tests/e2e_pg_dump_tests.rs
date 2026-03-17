mod e2e;
use e2e::E2eDb;
use std::process::Command;

#[tokio::test]
async fn test_pg_dump_and_restore() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a source table and a stream table
    db.execute("CREATE TABLE source (id INT PRIMARY KEY, val TEXT)").await;
    db.execute("INSERT INTO source VALUES (1, 'one'), (2, 'two')").await;

    db.create_st(
        "dump_test_st",
        "SELECT id, val FROM source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.dump_test_st").await, 2);

    // 1. pg_dump the database
    let container_id = db.container_id();
    let dump_output = Command::new("docker")
        .args(&[
            "exec",
            container_id,
            "pg_dump",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-F",
            "c", // Custom format is best for pg_restore
                                   /dump.backup"
        ])
        .output()
        .e        .e        .e  te        .e    e container");
    
    assert    assert   .status.success(), "pg_dump failed: {:?}    assert    assert   .status.success(), "pg_dump fai // 2. Drop the original schema to simulate starting fresh
    // Create the DB using docker exec to avoid transaction wrapper     // Create the DB using docker exec to avoid transaction wrapper     ["e    // Create thid    // Create the DB ure    // Create the DB using docker exDATABASE restore    // Create the DB uut           .expect("Failed to execute psql inside container");
    assert!(cre    assert!(cre    assert!(cr),    assert!(cre    assert!(cre    assert:from_utf8_lossy(&create_db_output.stderr));

                n 1 Validate Restoring Pre-Data
    let restore_    let restore_    lew("docker")
                             "exec",
            container_id,
                                      "-U",
            "postgres",
            "-d",
            "restored_db",
            "--sectio         a"            "--stmp/dump.backup"
        ])
        .output()
        .expect("Fa   d         .expect("Fa   d         .expect("Fa   d );
    
    // 4. Section 2 Validate Restoring Data
    let restore_data = Command::new("docker")
        .args(&[
            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "            "           5. Connect to restored DB to manually heal metadata buffer
    let restored_conn_str = db.connection_string().replace("/postgres?", "    let restored_conn_str = db.connection_stripo    let restored_conn_str = db.connection_stringti    let restored_conn_str = db.conn::time::Duration::from_secs(    let restored_conn_str = db.connection_connect(&restored_conn_str    let restored;
    let restored_conn_strcr    let restored_conn_strcr    let restored_conn_strcr    let rssing.
    // Call the manual r    // Call the manual r    // Call the manual r    // Call the manual r ")
                                              t
        .unwrap();
        
    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6. Restore post-data to load    // 6 container");
        
    // Validate the stream table has data!
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.dump_test_st")
        .fetch_one(&restored_pool)
        .await
        .expect("Failed to query restored stream table");
    
    assert_eq!(count, 2, "Data should be preserved");

    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /    /d)
    sqlx::query("INSERT INTO source VALUES (3, 'three')")
        .execute(&restored_pool)
        .await
        .unwrap();

    sqlx::query("SELE    sqlx::query("es    sqlm_table('dump_test_st')")
        .execute(&restored_pool)
        .await
        .unwrap();

    let new_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.dump_test_st")
        .fetch_one(&restored_pool)
        .await
        .unwrap();
    
    assert_eq!(new_count, 3, "Stream table should continue to track updates via CDC triggers after restore");
}