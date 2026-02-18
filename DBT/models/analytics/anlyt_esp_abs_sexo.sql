WITH get_src_abs_sex AS (
    SELECT * FROM {{ ref('src_abs_sex') }}
)

SELECT
    "año",
    "participación",
    CASE 
        WHEN sexo = 'Hombres' THEN 'Masculino'
        WHEN sexo = 'Mujeres' THEN 'Femenino'
        ELSE sexo 
    END as sexo,
    electores
FROM get_src_abs_sex