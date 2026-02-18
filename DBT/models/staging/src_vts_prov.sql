WITH raw_votopartido_provincia AS (
  SELECT * FROM ELECTIONS.RAW.RAW_VOTO_PARTIDO_PROVINCIA_1RARONDA
),seed_siglas AS (
    SELECT * FROM {{ ref('siglas') }}
),
votopartido_provincia_with_acronym as (

    SELECT
    "año",
    votos,
    provincia,
    CONCAT(raw_votopartido_provincia.partido,' (',seed_siglas.siglas,')') as partido
    from raw_votopartido_provincia
    left join seed_siglas on seed_siglas.partido = raw_votopartido_provincia.partido

)
SELECT * FROM votopartido_provincia_with_acronym