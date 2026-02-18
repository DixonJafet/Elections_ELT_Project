WITH get_src_pbl_prov AS (
    SELECT * FROM {{ ref('src_pbl_prov') }}
)

SELECT
    "año" as year,
    provincia as province,
    "población" as population,
    electores as voters
FROM get_src_pbl_prov