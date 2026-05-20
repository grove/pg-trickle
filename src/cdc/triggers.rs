//! QUAL-3 (v0.60.0): Trigger SQL builders and column-name escaping logic.
//!
//! This module contains pure-logic helpers extracted from `cdc.rs` as part of
//! the QUAL-3 module decomposition.  All functions here are either pure (no SPI)
//! or are the trigger DDL builders that depend on SPI only for relation lookup.

// ── Reserved change-buffer column names ────────────────────────────────────

/// Built-in CDC metadata column names that live at the top of every change-buffer
/// table.  A source table column with any of these names would collide with the
/// metadata column in a flat (A44-10 D+I) change-buffer schema.
pub(super) const RESERVED_CB_COLS: &[&str] =
    &["change_id", "lsn", "action", "pk_hash", "changed_cols"];

/// Map a *source* column name to its change-buffer storage name.
///
/// When a source column name matches one of [`RESERVED_CB_COLS`], the column is
/// stored in the change buffer as `__usr_{name}` to prevent a
/// `column "…" specified more than once` error in PostgreSQL.
/// All other names pass through unchanged.
pub fn cb_col_name(name: &str) -> String {
    if RESERVED_CB_COLS.contains(&name) {
        format!("__usr_{name}")
    } else {
        name.to_string()
    }
}

/// Build the VARBIT bitmask expression for `changed_cols` in UPDATE triggers.
///
/// Each non-PK column gets a `CASE WHEN NEW.col IS DISTINCT FROM OLD.col THEN
/// B'1' ELSE B'0' END)::varbit` bit that is concatenated with `||`.
///
/// Returns `None` for keyless tables (`pk_columns` empty): all columns
/// contribute to the content hash and must always be present.
///
/// For INSERT and DELETE rows `changed_cols` is stored as `NULL`, indicating
/// that all new_*/old_* column values are populated (backward-compatible).
pub fn build_changed_cols_bitmask_expr(
    pk_columns: &[String],
    columns: &[(String, String)],
) -> Option<String> {
    if pk_columns.is_empty() {
        return None; // keyless: must always write all columns
    }
    let parts: Vec<String> = columns
        .iter()
        .map(|(col_name, type_name)| {
            let qcol = col_name.replace('"', "\"\"");
            // pgvector types (vector, halfvec, sparsevec) do not define an '='
            // operator, so IS DISTINCT FROM (which uses '=') would fail.
            // Cast to text for comparison — text always supports equality.
            let base_type = type_name.split('(').next().unwrap_or("").trim();
            let is_pgvector = matches!(base_type, "vector" | "halfvec" | "sparsevec");
            if is_pgvector {
                format!(
                    "(CASE WHEN NEW.\"{qcol}\"::text IS DISTINCT FROM OLD.\"{qcol}\"::text \
                     THEN B'1' ELSE B'0' END)::varbit"
                )
            } else {
                format!(
                    "(CASE WHEN NEW.\"{qcol}\" IS DISTINCT FROM OLD.\"{qcol}\" \
                     THEN B'1' ELSE B'0' END)::varbit"
                )
            }
        })
        .collect();
    Some(parts.join(" ||\n        "))
}

/// CDC-6 (v0.65.0): Set up an "inlined-data" AFTER trigger adapter for a
/// DuckLake inlined-data table.
///
/// DuckLake stores small datasets inline in the metadata catalog rather than
/// as Parquet files. When rows are inserted/updated/deleted in an inlined-data
/// table, the DuckLake catalog itself is modified — but the table is a
/// regular writable PostgreSQL relation that supports triggers.
///
/// This function creates AFTER INSERT/UPDATE/DELETE triggers on the inlined
/// table so that changes are captured into the standard change buffer, enabling
/// differential refresh of any stream table that depends on it.
pub fn setup_ducklake_inlined_data_trigger(
    inlined_table_oid: pgrx::pg_sys::Oid,
    pgt_id: i64,
    change_schema: &str,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    let oid_u32 = inlined_table_oid.to_u32();
    let stable_name = crate::citus::stable_name_for_oid(inlined_table_oid)
        .unwrap_or_else(|_| oid_u32.to_string());
    let change_table = format!("\"{change_schema}\".changes_{stable_name}");
    let trigger_fn = format!("pgtrickle.pgt_capture_trigger_{stable_name}");
    let qualified = Spi::get_one_with_args::<String>(
        "SELECT $1::oid::regclass::text",
        &[(oid_u32 as i64).into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::NotFound(format!("Inlined-data table with OID {oid_u32} not found"))
    })?;

    // Create the standard change buffer table.
    let col_defs = super::resolve_source_column_defs(inlined_table_oid)?;
    super::create_change_buffer_table(inlined_table_oid, change_schema, &col_defs, &stable_name)?;

    // Create a trigger function that writes to the change buffer.
    let create_fn_sql = format!(
        "CREATE OR REPLACE FUNCTION {trigger_fn}() \
         RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
             IF TG_OP = 'INSERT' THEN \
                 INSERT INTO {change_table} (lsn, action, pk_hash) \
                 VALUES (pg_current_wal_insert_lsn(), 'I', \
                         pgtrickle.pg_trickle_hash(NEW::text)); \
                 RETURN NEW; \
             ELSIF TG_OP = 'DELETE' THEN \
                 INSERT INTO {change_table} (lsn, action, pk_hash) \
                 VALUES (pg_current_wal_insert_lsn(), 'D', \
                         pgtrickle.pg_trickle_hash(OLD::text)); \
                 RETURN OLD; \
             ELSE \
                 INSERT INTO {change_table} (lsn, action, pk_hash) \
                 VALUES (pg_current_wal_insert_lsn(), 'D', \
                         pgtrickle.pg_trickle_hash(OLD::text)); \
                 INSERT INTO {change_table} (lsn, action, pk_hash) \
                 VALUES (pg_current_wal_insert_lsn(), 'I', \
                         pgtrickle.pg_trickle_hash(NEW::text)); \
                 RETURN NEW; \
             END IF; \
         END; $$"
    );
    Spi::run(&create_fn_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Attach the trigger to the inlined-data table.
    let trigger_name = format!("pgt_ducklake_inlined_{pgt_id}");
    let create_trig_sql = format!(
        "CREATE TRIGGER \"{trigger_name}\" \
         AFTER INSERT OR UPDATE OR DELETE ON {qualified} \
         FOR EACH ROW EXECUTE FUNCTION {trigger_fn}()"
    );
    Spi::run(&create_trig_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── cb_col_name tests ─────────────────────────────────────────────────

    #[test]
    fn test_cb_col_name_escapes_reserved_pgt_prefix() {
        // All RESERVED_CB_COLS must be prefixed with __usr_
        assert_eq!(cb_col_name("action"), "__usr_action");
        assert_eq!(cb_col_name("lsn"), "__usr_lsn");
        assert_eq!(cb_col_name("pk_hash"), "__usr_pk_hash");
        assert_eq!(cb_col_name("changed_cols"), "__usr_changed_cols");
        assert_eq!(cb_col_name("change_id"), "__usr_change_id");
    }

    #[test]
    fn test_cb_col_name_passes_through_normal_name() {
        assert_eq!(cb_col_name("id"), "id");
        assert_eq!(cb_col_name("order_id"), "order_id");
        assert_eq!(cb_col_name("status"), "status");
        assert_eq!(cb_col_name("amount"), "amount");
    }

    // ── build_changed_cols_bitmask_expr tests ────────────────────────────

    #[test]
    fn test_build_changed_cols_bitmask_expr_single_col() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            ("val".to_string(), "text".to_string()),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("IS DISTINCT FROM"),
            "should use IS DISTINCT FROM: {expr}"
        );
        assert!(expr.contains("::varbit"), "should use VARBIT type: {expr}");
    }

    #[test]
    fn test_build_changed_cols_bitmask_expr_empty() {
        // Keyless table (empty pk) → None
        let cols = vec![("a".to_string(), "int".to_string())];
        assert!(build_changed_cols_bitmask_expr(&[], &cols).is_none());
    }

    #[test]
    fn test_build_changed_cols_bitmask_expr_pgvector_casts_to_text() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            ("embedding".to_string(), "vector".to_string()),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("::text"),
            "pgvector col should cast to text: {expr}"
        );
    }
}
