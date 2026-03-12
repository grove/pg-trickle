{{ config(
    materialized  = 'stream_table',
    schedule      = none,
    refresh_mode  = 'DIFFERENTIAL'
) }}

WITH RECURSIVE tree AS (
    SELECT
        id,
        name,
        parent_id,
        name      AS path,
        0         AS depth
    FROM {{ ref('stg_departments') }}
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
        d.id,
        d.name,
        d.parent_id,
        tree.path || ' > ' || d.name  AS path,
        tree.depth + 1
    FROM {{ ref('stg_departments') }} d
    JOIN tree ON d.parent_id = tree.id
)
SELECT id, name, parent_id, path, depth FROM tree
