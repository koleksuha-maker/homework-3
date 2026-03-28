import pandas as pd
import glob
import os
from sqlalchemy import create_engine
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_parquet_to_postgres():
    path = '/opt/airflow/data' 
    all_files = glob.glob(os.path.join(path, "*.parquet"))
    engine = create_engine('postgresql://postgres:041101@project_database:5432/final_project')

    try:
        for f in all_files:
            df = pd.read_parquet(f)

            def upload_unique_data(data, table_name, pk_column):
                data = data.drop_duplicates(subset=[pk_column])
                try:
                    data.to_sql(table_name, engine, if_exists='append', index=False)
                except Exception as e:
                    print(f"Error loading to {table_name}: {e}")

            upload_unique_data(df[['user_id', 'user_phone']], 'User', 'user_id')
            upload_unique_data(df[['store_id', 'store_address']], 'store', 'store_id')
            upload_unique_data(df[['item_id', 'item_title', 'item_category']], 'item', 'item_id')
            upload_unique_data(df[['driver_id', 'driver_phone']], 'driver', 'driver_id')

            orders = df[['order_id', 'user_id', 'store_id', 'order_discount', 
                        'order_cancellation_reason', 'created_at', 'paid_at', 
                        'canceled_at', 'payment_type']].drop_duplicates(subset=['order_id'])
            orders.to_sql('Order', engine, if_exists='append', index=False)

            delivery = df[['order_id', 'delivery_started_at', 'delivered_at', 
                           'delivery_cost', 'address_text']].drop_duplicates(subset=['order_id'])
            delivery.to_sql('delivery', engine, if_exists='append', index=False)

            df[['order_id', 'driver_id']].drop_duplicates().to_sql('order_driver', engine, if_exists='append', index=False)
            df[['order_id', 'item_id', 'item_quantity', 'item_price', 
                'item_discount', 'item_canceled_quantity', 'item_replaced_id']].to_sql('Order_Item', engine, if_exists='append', index=False)
    finally:
        engine.dispose()

with DAG(
    dag_id='my_loading_data_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_run_loading = PythonOperator(
        task_id='load_parquet_task',
        python_callable=load_parquet_to_postgres
    )