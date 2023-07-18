"""
SQLITE COMMANDS FOR DB OPERATIONS :
note : Any query you will run is the same as standard SQL query structure.

- .schema {table_name} # to check the schema of "table_name"  table in your sqlite DB.
- .tables # shows available tables 
- 
"""


from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago

default_args ={
    'owner':'Pratik'
}

with DAG(
    dag_id="executing_sql_pipeline",
    description='Pipeline using SQL operators',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['pipeline','sql']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER NOT NULL,
            city VARCHAR(50),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
        sqlite_conn_id='my_sqlite_conn', # connection ID which you have given an ID name when connection initialization from Airflow UI page 
        dag=dag
    )

    #insert value task
    insert_values_1 = SqliteOperator( #insert value task
        task_id = "insert_values_1",
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES 
            ('PRATIK','23',false),
            ('krishna','24',true),
            ('harsh','20',true);
                """,
        sqlite_conn_id='my_sqlite_conn',# connection ID which you have given an ID name when connection initialization from Airflow UI page 
        dag=dag
    )
    #insert value task
    insert_values_2 = SqliteOperator(
        task_id = "insert_values_2",
        sql = r"""
            INSERT INTO users (name, age) VALUES 
            ('PRATIK2','23',
            ('krishna2','24'),
            ('harsh2','20');
                """,
        sqlite_conn_id='my_sqlite_conn',# connection ID which you have given an ID name when connection initialization from Airflow UI page 
        dag=dag
    )
    #delete value task
    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
        """,
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )
    #update value task
    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r"""
            UPDATE users SET city = 'Toronto';
        """,
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    #display value task
    display_result = SqliteOperator(
        task_id = "display_result",
        sql = r"""SELECT * FROM users;""",
        sqlite_conn_id='my_sqlite_conn',# connection ID which you have given an ID name when connection initialization from Airflow UI page 
        dag=dag,
        do_xcomm_push = True # the result of this task will be push to the "do_xcomm_push" variable that can be used other place.
    )
    #insert value task


create_table >> [insert_values_1,insert_values_2] >> delete_values >> update_values >> display_result # pipeline order.