WITH raw_abstencionismo_edad AS (
  SELECT * FROM ELECTIONS.RAW.RAW_ELECTORES_VOTOABS_EDAD
)

SELECT 
    "año",
    CASE 
        WHEN edad = '18-30' THEN '18-29'
        ELSE edad 
    END as edad,
    electores,
    "participación",


FROM raw_abstencionismo_edad