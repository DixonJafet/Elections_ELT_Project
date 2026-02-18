WITH get_src_vts_prov_2da AS (
    SELECT * FROM {{ ref('src_vts_prov_2da') }}
)

SELECT * FROM get_src_vts_prov_2da