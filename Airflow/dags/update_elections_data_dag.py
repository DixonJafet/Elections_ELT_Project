from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from pathlib import Path
import os


@dag
def update_elections_data():



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

   


    # Task 2: Trigger dbt Cloud transformation job
    transform_data = DbtCloudRunJobOperator(
        task_id="transform_data",
        dbt_cloud_conn_id="dbt_cloud_default", # Connection set in Airflow UI or Docker ENV
        job_id=70471823561707,                         # Your dbt Cloud Job ID
        wait_for_termination=True,             # Task stays 'running' until dbt job finishes
        check_interval=30,                     # Seconds between status checks
        timeout=3600                           # Timeout after 1 hour
    )

        

    extract_data >> transform_data


update_elections_data()