{{ config(materialized='view') }}

SELECT
    id,
    name,
    parent_id
FROM {{ ref('departments') }}
