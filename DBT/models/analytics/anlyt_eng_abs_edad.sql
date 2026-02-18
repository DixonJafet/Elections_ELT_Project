WITH get_src_abs_edad AS (
    SELECT * FROM {{ ref('src_abs_edad') }}
)

SELECT
    "año" as year, 
    CASE 
        WHEN "participación" = 'votos' THEN 'votes'
        WHEN "participación" = 'Abstencionismo' THEN 'Abstentionism'
        ELSE "participación" 
    END as participation,
    electores as voters,
    edad as age
FROM get_src_abs_edad