WITH get_src_abs_sex AS (
    SELECT * FROM {{ ref('src_abs_sex') }}
)

SELECT
    "año" as year,
    CASE 
        WHEN "participación" = 'votos' THEN 'votes'
        WHEN "participación" = 'Abstencionismo' THEN 'Abstentionism'
        ELSE "participación" 
    END as participation,
    CASE 
        WHEN sexo = 'Hombres' THEN 'Male'
        WHEN sexo = 'Mujeres' THEN 'Female'
        ELSE sexo 
    END as sex,
    electores as voters
FROM get_src_abs_sex