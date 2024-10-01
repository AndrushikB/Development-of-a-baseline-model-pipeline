

import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    dag_id='flats',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL_flats"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_flats_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    
    
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, Float, BigInteger, Boolean, UniqueConstraint
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        flats_character = Table(
        'flats_character',
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
        Column('price', BigInteger),
        Column('target', BigInteger),
        UniqueConstraint('building_id', name='unique_building_constraint')
        )

        from sqlalchemy import inspect

        if not inspect(db_conn).has_table(flats_character.name): 
            metadata.create_all(db_conn)
        
    @task()
    def extract():

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select
            f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio, f.total_area, f.price, f.building_id,
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
        from flats as f
        left join buildings as b on b.id = f.building_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        temp = data.drop('building_type_int', axis=1)
        temp.replace({0: None}, inplace=True)
        temp['building_type_int'] = data['building_type_int']
        data = temp
        data['target'] = data['price']
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_character",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id'],
            rows=data.values.tolist()
    )

    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_flats_dataset()