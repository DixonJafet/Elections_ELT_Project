WITH get_src_vts_prov AS (
    SELECT * FROM {{ ref('src_vts_prov') }}
)

SELECT
    "año" as year,
    provincia as province,
    partido as party,
    votos as votes

FROM get_src_vts_prov