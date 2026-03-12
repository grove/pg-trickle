-- department_report payroll must equal the sum of direct employee
-- salaries for each top-level division.
WITH expected AS (
    SELECT
        split_part(t.path, ' > ', 2)  AS division,
        SUM(e.salary)                  AS total_payroll,
        COUNT(e.id)                    AS total_headcount
    FROM {{ ref('department_tree') }}   t
    JOIN {{ ref('stg_employees') }}     e  ON e.department_id = t.id
    WHERE t.depth >= 1
    GROUP BY 1
),
actual AS (
    SELECT division, total_payroll, total_headcount
    FROM {{ ref('department_report') }}
)
SELECT e.*
FROM expected e
LEFT JOIN actual a USING (division)
WHERE
    a.division IS NULL
    OR e.total_payroll    <> a.total_payroll
    OR e.total_headcount  <> a.total_headcount
