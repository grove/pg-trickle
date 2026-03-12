-- department_stats headcount must match a direct COUNT(*) from employees.
-- Returns rows where the incremental result diverges from the ground truth.
WITH ground_truth AS (
    SELECT
        department_id,
        COUNT(*)         AS headcount,
        SUM(salary)      AS total_salary
    FROM {{ ref('stg_employees') }}
    GROUP BY department_id
),
stream AS (
    SELECT department_id, headcount, total_salary
    FROM {{ ref('department_stats') }}
    WHERE headcount > 0
)
SELECT
    g.department_id,
    g.headcount           AS expected_headcount,
    s.headcount           AS actual_headcount,
    g.total_salary        AS expected_salary,
    s.total_salary        AS actual_salary
FROM ground_truth g
LEFT JOIN stream s USING (department_id)
WHERE
    s.department_id IS NULL
    OR g.headcount    <> s.headcount
    OR g.total_salary <> s.total_salary
