-- Snowflake user creation
-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS DATA_WH;
GRANT OPERATE ON WAREHOUSE DATA_WH TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD=''
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='DATA_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='ELECTIONS.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

-- Step 5: Create a database and schema for the MovieLens project
CREATE DATABASE IF NOT EXISTS ELECTIONS;
CREATE SCHEMA IF NOT EXISTS ELECTIONS.RAW;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE DATA_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE ELECTIONS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE ELECTIONS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ELECTIONS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ELECTIONS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ELECTIONS.RAW TO ROLE TRANSFORM;


CREATE STAGE electionsstage
  URL='s3://votes-cr-records'
  CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');


USE WAREHOUSE DATA_WH;
USE DATABASE ELECTIONS;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_abstencionismo_provincia(
  "año" INTEGER,
  provincia STRING,
  abstencionismo FLOAT
);

CREATE OR REPLACE TABLE raw_electores_votoabs_edad(
  "año" INTEGER,
  edad STRING,
  "participación" STRING,
  electores INTEGER
);

CREATE OR REPLACE TABLE raw_electores_votoabs_sexo(
  "año" INTEGER, 
  "participación" STRING,
  electores INTEGER,
  sexo STRING
);

CREATE OR REPLACE TABLE raw_electoresypoblacion_provincia(
  "año" INTEGER, 
  provincia STRING,
  "población" INTEGER,
  electores INTEGER
);

CREATE OR REPLACE TABLE raw_voto_partido_provincia_1raronda(
  "año" INTEGER, 
  provincia STRING,
  partido STRING,
  votos INTEGER
);

CREATE OR REPLACE TABLE raw_voto_partido_provincia_2daronda(
  "año" INTEGER, 
  provincia STRING,
  partido STRING,
  votos INTEGER
);

COPY INTO raw.raw_abstencionismo_provincia
FROM '@raw.electionsstage/Abstencionismo_Provincia.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');



COPY INTO raw.raw_electores_votoabs_edad
FROM '@raw.electionsstage/Electores_VotoAbs_Edad.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');


COPY INTO raw.raw_electores_votoabs_sexo
FROM '@raw.electionsstage/Electores_VotoAbs_Sexo.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');


COPY INTO raw.raw_electoresypoblacion_provincia
FROM '@raw.electionsstage/ElectoresYPoblacion_Provincia.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');


COPY INTO raw.raw_voto_partido_provincia_1raronda
FROM '@raw.electionsstage/Voto_Partido_Provincia_1raRonda.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');


COPY INTO raw.raw_voto_partido_provincia_2daronda
FROM '@raw.electionsstage/Voto_Partido_Provincia_2daRonda.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ENCODING = 'UTF-8');