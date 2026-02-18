WITH get_src_abs_edad AS (
    SELECT * FROM {{ ref('src_abs_edad') }}
)

SELECT * FROM get_src_abs_edad