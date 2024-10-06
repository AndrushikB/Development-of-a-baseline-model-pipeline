
import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id='clean_flats',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL_flats"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from steps.data_preprocessing import remove_duplicates, fill_missing_values, delete_outliers

    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, Float, Numeric, Boolean, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        clean_flats = Table(
        'clean_flats',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('building_id', Integer),
        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', Boolean),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', Boolean),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', Numeric),
        Column('target', Numeric),
        UniqueConstraint('building_id', name='unique_building_constraint_clean')
        )
        if not inspect(db_engine).has_table(clean_flats.name):
            metadata.create_all(db_engine)
    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = 'select * from flats_character'
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        
        removed_data = remove_duplicates(data)
        filled_data = fill_missing_values(removed_data)
        data = delete_outliers(filled_data)
        
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table= "clean_flats",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()
