WITH get_src_vts_prov AS (
    SELECT * FROM {{ ref('src_vts_prov') }}
)

SELECT * FROM get_src_vts_prov