-- Every department's path must start with 'Company' (the root).
-- Every non-root department's path must contain ' > '.
-- Returns rows that violate either condition. Empty result = pass.
SELECT id, name, path, depth
FROM {{ ref('department_tree') }}
WHERE
    path NOT LIKE 'Company%'
    OR (depth > 0 AND path NOT LIKE '% > %')
