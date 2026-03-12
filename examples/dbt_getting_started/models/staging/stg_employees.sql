{{ config(materialized='view') }}

SELECT
    id,
    name,
    department_id,
    salary
FROM {{ ref('employees') }}
