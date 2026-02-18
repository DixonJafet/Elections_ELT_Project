WITH get_src_vts_prov_2da AS (
    SELECT * FROM {{ ref('src_vts_prov_2da') }}
)

SELECT
    "año" as year,
    provincia as province,
    partido as party,
    votos as votes

FROM get_src_vts_prov_2da