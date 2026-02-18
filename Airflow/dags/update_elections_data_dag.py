from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from pathlib import Path
import os


@dag
def update_elections_data():
    DBT_PROJECT_PATH = Path(os.getenv("DBT_ROOT_PATH", "/opt/airflow/dbt"))

    profile_config = ProfileConfig(
        profile_name="Snowflake_Analytics_Elections",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="my_snowflake", # Matches the ID in Airflow UI
            profile_args={
                "database": "ELECTIONS",
                "schema": "DBT_JDIXON",
            },
        ),
    )



    extract_data = SQLExecuteQueryOperator(
        task_id="extract_elections_data",
        conn_id="my_snowflake",
        sql="""
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
        """
        )

   


    transform_data = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,

    )

        

    extract_data >> transform_data


update_elections_data()