//! Row ID generation strategies for different operators.
//!
//! Row ID computation depends on the operator that produces the row.
//! See `PLAN.md` Phase 6.2 for the full strategy table.

/// Strategies for computing row IDs at each operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowIdStrategy {
    /// Use the primary key columns of the source table.
    PrimaryKey { pk_columns: Vec<String> },
    /// Hash all columns (fallback when no PK is available).
    AllColumns { columns: Vec<String> },
    /// Combine two child row IDs (for joins).
    CombineChildren,
    /// Hash the group-by columns (for aggregates).
    GroupByKey { group_columns: Vec<String> },
    /// Pass through the child's row ID (for project/filter).
    PassThrough,
}

#[cfg(test)]
mod tests {
    use super::RowIdStrategy;

    #[test]
    fn test_row_id_strategy_debug_includes_variant_and_columns() {
        let strategy = RowIdStrategy::PrimaryKey {
            pk_columns: vec!["tenant_id".to_string(), "order_id".to_string()],
        };

        let debug = format!("{strategy:?}");

        assert!(debug.contains("PrimaryKey"));
        assert!(debug.contains("tenant_id"));
        assert!(debug.contains("order_id"));
    }

    #[test]
    fn test_row_id_strategy_clone_preserves_group_columns() {
        let original = RowIdStrategy::GroupByKey {
            group_columns: vec!["region".to_string(), "year".to_string()],
        };

        assert_eq!(original.clone(), original);
    }

    #[test]
    fn test_row_id_strategy_all_columns_preserves_column_order() {
        let strategy = RowIdStrategy::AllColumns {
            columns: vec![
                "id".to_string(),
                "created_at".to_string(),
                "payload".to_string(),
            ],
        };

        assert_eq!(
            strategy,
            RowIdStrategy::AllColumns {
                columns: vec![
                    "id".to_string(),
                    "created_at".to_string(),
                    "payload".to_string(),
                ],
            }
        );
    }

    #[test]
    fn test_row_id_strategy_unit_variants_compare_by_variant() {
        assert_eq!(
            RowIdStrategy::CombineChildren,
            RowIdStrategy::CombineChildren
        );
        assert_eq!(RowIdStrategy::PassThrough, RowIdStrategy::PassThrough);
    }
}
