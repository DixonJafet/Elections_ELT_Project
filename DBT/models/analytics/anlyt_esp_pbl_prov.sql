
WITH get_src_pbl_prov AS (
    SELECT * FROM {{ ref('src_pbl_prov') }}
)

SELECT * FROM get_src_pbl_prov