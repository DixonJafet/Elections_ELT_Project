WITH raw_abstencionismo_provincia AS (
  SELECT * FROM ELECTIONS.RAW.RAW_ABSTENCIONISMO_PROVINCIA
)

SELECT
  "año" AS "año",
  provincia,
  abstencionismo
FROM raw_abstencionismo_provincia
