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
            "-f",
            "/tmp/dump.backup"
        ])
        ])
 "/tmp/dump.backup"
at is best to execute pg_dump inside at is best to execute pg_ertat is best to.status.success(), "pg_dump failed: {:?}", String::from_utf8_lossy(&dump_output.stderr));

    //    //    //    //    //    //    //    //    in    //    //    //    //    //    //    //    //    in    //    //    //  yt    //    //    //    //    //    //    //    //    in    //    //    //       //    //    TE DATABASE restored_db").await;

    // 3. pg_restore into the     // 3. pg
    let restore_output = Command::new("docke    let restore_output = Command::new("docke    let resttai    let restore_output = Commre    let restore_output = Command::new("des    let restore_output = Command::new("dockdb    let restore"-    let restore_ousa    let restore_output =dump.backup"
        ])
        .output()
                        t                tore inside container");
    
    // We don'    // We don'    // Wess because sometimes pg_restore emits warnings about    // We don'    // We don' //    // Wean check if it at least exited with some info. Wait, single trans won't fail if the schema exists
    // actually it might. Let's just check the status or error.
    if !restore_output.status.success() {
        println!("pg_restore stderr: {:?}", String::from_utf8_lossy(&restore_output.stderr));
    }

    // Now connect to the restored_db
    // E2eDb has the connection pool bound to 'postgres' db. So we create a new pool to 'restored_db'
    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    le t    let rts    let rest          let rest    let rest    let rest    let rest    lpl    let rc.dump_    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    let rest    let rest    let rest    let rest    lpl    lshould be set up!
    // Let's modify the source table and see if ST refreshes correctly!
    sqlx::query("INSERT INTO source VALUES (3, 'three')")
        .execute(&restored_pool)
        .await
        .unwr        .unwr        .unwr        .uck        .unwr        .unwr        .unw)")
        .execute(&restored_pool)
        .await
        .unwrap();

    let new_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.dump_test_st")
        .fetch_one(&restored_pool)
        .await
        .unwrap();
    
    assert_eq!(new_count, 3, "Stream table should continue to track updates via CDC triggers after restore");
}
