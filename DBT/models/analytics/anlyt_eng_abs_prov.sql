
WITH get_src_abs_prov AS (
    SELECT * FROM {{ ref('src_abs_prov') }}
)

SELECT
    "año" as year,
    provincia as province,
    abstencionismo as abstentionism
FROM get_src_abs_prov