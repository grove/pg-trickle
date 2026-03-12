{{ config(
    materialized  = 'stream_table',
    schedule      = none,
    refresh_mode  = 'DIFFERENTIAL'
) }}

SELECT
    t.id                                AS department_id,
    t.name                              AS department_name,
    t.path                              AS full_path,
    t.depth,
    COUNT(e.id)                         AS headcount,
    COALESCE(SUM(e.salary),  0)         AS total_salary,
    COALESCE(AVG(e.salary),  0)         AS avg_salary
FROM {{ ref('department_tree') }} t
LEFT JOIN {{ ref('stg_employees') }} e
       ON e.department_id = t.id
GROUP BY t.id, t.name, t.path, t.depth
