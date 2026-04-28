// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub fn execute_topk_refresh(st: &StreamTableMeta) -> Result<(i64, i64), PgTrickleError> {
    // G12-ERM-1: Record the effective mode for this execution path.
    set_effective_mode("TOP_K");

    // EC-25/EC-26: Ensure the internal_refresh flag is set so DML guard
    // triggers allow the refresh executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;

    let topk_limit = st.topk_limit.ok_or_else(|| {
        PgTrickleError::InternalError("execute_topk_refresh called on non-TopK stream table".into())
    })?;
    let topk_order_by = st.topk_order_by.as_deref().ok_or_else(|| {
        PgTrickleError::InternalError("TopK stream table missing order_by metadata".into())
    })?;

    // G12-2: TopK runtime validation — re-parse the reconstructed full query
    // and verify the detected TopK pattern matches stored catalog metadata.
    // On mismatch, fall back to FULL refresh to prevent silent correctness issues.
    if let Err(reason) = validate_topk_metadata(
        &st.defining_query,
        topk_limit,
        topk_order_by,
        st.topk_offset,
    ) {
        pgrx::warning!(
            "pg_trickle: TopK metadata inconsistency for {}.{}: {}. \
             Falling back to FULL refresh.",
            schema,
            name,
            reason,
        );
        set_effective_mode("FULL");
        return execute_full_refresh(st);
    }

    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );

    // Reconstruct the full TopK query from base query + ORDER BY + LIMIT [+ OFFSET].
    let topk_query = if let Some(offset) = st.topk_offset {
        format!(
            "{} ORDER BY {} LIMIT {} OFFSET {}",
            st.defining_query, topk_order_by, topk_limit, offset
        )
    } else {
        format!(
            "{} ORDER BY {} LIMIT {}",
            st.defining_query, topk_order_by, topk_limit
        )
    };

    // Compute row_id using the same hash formula as normal refresh.
    let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);

    // Build the source subquery with row IDs.
    // Use alias `sub` to match what row_id_expr_for_query() generates.
    let source_sql = format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({topk_query}) sub");

    // Get column names from the storage table (excluding __pgt_row_id).
    let columns = crate::dvm::get_defining_query_columns(&st.defining_query)?;

    // Build the MERGE statement.
    let col_list: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect();

    let update_set: Vec<String> = col_list
        .iter()
        .map(|c| format!("{c} = __pgt_topk_src.{c}"))
        .collect();

    let insert_cols: String = std::iter::once("__pgt_row_id".to_string())
        .chain(col_list.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    let insert_vals: String = std::iter::once("__pgt_topk_src.__pgt_row_id".to_string())
        .chain(col_list.iter().map(|c| format!("__pgt_topk_src.{c}")))
        .collect::<Vec<_>>()
        .join(", ");

    // Build an IS DISTINCT FROM check for change detection in WHEN MATCHED.
    let is_distinct_check = if col_list.is_empty() {
        "TRUE".to_string()
    } else {
        col_list
            .iter()
            .map(|c| format!("{quoted_table}.{c}::text IS DISTINCT FROM __pgt_topk_src.{c}::text"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };

    let merge_sql = format!(
        "MERGE INTO {quoted_table} \
         USING ({source_sql}) AS __pgt_topk_src \
         ON {quoted_table}.__pgt_row_id = __pgt_topk_src.__pgt_row_id \
         WHEN MATCHED AND ({is_distinct_check}) THEN \
           UPDATE SET {update_set} \
         WHEN NOT MATCHED THEN \
           INSERT ({insert_cols}) VALUES ({insert_vals}) \
         WHEN NOT MATCHED BY SOURCE THEN \
           DELETE",
        update_set = update_set.join(", "),
    );

    let (rows_inserted, rows_deleted) = Spi::connect_mut(|client| {
        let result = client
            .update(&merge_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // MERGE returns total rows processed. We don't get separate insert/delete
        // counts from SPI, so return the total as "inserted" and 0 as "deleted".
        // The actual bookkeeping is approximate here.
        Ok::<(i64, i64), PgTrickleError>((result.len() as i64, 0))
    })?;

    pgrx::debug1!(
        "[pg_trickle] TopK refresh of {}.{}: MERGE processed {} rows",
        schema,
        name,
        rows_inserted,
    );

    Ok((rows_inserted, rows_deleted))
}
