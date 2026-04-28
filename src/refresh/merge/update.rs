// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub(crate) fn check_proc_hashes_changed(st: &StreamTableMeta) -> bool {
    let funcs = match &st.functions_used {
        Some(f) if !f.is_empty() => f,
        _ => return false,
    };

    // Build current hash map: { func_name → md5(prosrc concatenated) }
    let mut current_map: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for func_name in funcs {
        let hash_opt = Spi::get_one_with_args::<String>(
            "SELECT md5(string_agg(prosrc || coalesce(probin::text, ''), ',' ORDER BY oid)) \
             FROM pg_catalog.pg_proc \
             WHERE proname = $1",
            &[func_name.as_str().into()],
        )
        .unwrap_or(None);

        if let Some(h) = hash_opt {
            current_map.insert(func_name.to_lowercase(), h);
        }
    }

    // Serialize current map to JSON text.
    let current_json = match serde_json::to_string(&current_map) {
        Ok(j) => j,
        Err(e) => {
            pgrx::debug1!("[pg_trickle] EC-16: failed to serialize function hashes: {e}");
            return false;
        }
    };

    // Compare against stored hashes.
    match &st.function_hashes {
        None => {
            // First-time baseline: store and report no change.
            if let Err(e) = crate::catalog::StreamTableMeta::update_function_hashes(
                st.pgt_id,
                Some(&current_json),
            ) {
                pgrx::debug1!("[pg_trickle] EC-16: failed to store initial function hashes: {e}");
            }
            false
        }
        Some(stored) => {
            if *stored == current_json {
                false
            } else {
                // Hash changed — persist new hashes before returning.
                if let Err(e) = crate::catalog::StreamTableMeta::update_function_hashes(
                    st.pgt_id,
                    Some(&current_json),
                ) {
                    pgrx::debug1!("[pg_trickle] EC-16: failed to update function hashes: {e}");
                }
                true
            }
        }
    }
}
