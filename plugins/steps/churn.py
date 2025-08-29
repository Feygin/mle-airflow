# plugins/steps/churn.py
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (
    MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint, inspect
)

def create_table():
    """
    Create destination table if not exists.
    Writes to alt_users_churn as requested.
    """
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()

    users_churn_table = Table(
        'alt_users_churn',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        UniqueConstraint('customer_id', name='unique_customer_id_alt'),
    )

    if not inspect(engine).has_table(users_churn_table.name):
        metadata.create_all(engine)

def extract():
    """
    Extract data from source_db.
    Returns a pandas DataFrame via XCom.
    """
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
    select
        c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
        i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
        p.gender, p.senior_citizen, p.partner, p.dependents,
        ph.multiple_lines
    from contracts as c
    left join internet as i on i.customer_id = c.customer_id
    left join personal as p on p.customer_id = c.customer_id
    left join phone as ph on ph.customer_id = c.customer_id
    """
    try:
        data = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return data

def transform(data: pd.DataFrame):
    """
    Transform the extracted DataFrame.
    """
    data = data.copy()
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'].replace({'No': None}, inplace=True)
    return data

def load(data: pd.DataFrame):
    """
    Load transformed data into destination_db.alt_users_churn.
    """
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist(),
    )
