from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JDBC_URL = "jdbc:postgresql://project_database:5432/final_project"
DB_PROPERTIES = {
    "user": "postgres", 
    "password": "041101", 
    "driver": "org.postgresql.Driver"
}

def get_spark_session():
    return (SparkSession.builder
        .appName("Airflow_PySpark_Datamart")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate())

def run_full_analytics():
    spark = get_spark_session()
    orders = spark.read.jdbc(JDBC_URL, '"Order"', properties=DB_PROPERTIES)
    order_items = spark.read.jdbc(JDBC_URL, '"Order_Item"', properties=DB_PROPERTIES)
    stores = spark.read.jdbc(JDBC_URL, 'store', properties=DB_PROPERTIES)
    delivery = spark.read.jdbc(JDBC_URL, 'delivery', properties=DB_PROPERTIES)
    order_driver = spark.read.jdbc(JDBC_URL, 'order_driver', properties=DB_PROPERTIES)
    items_catalog = spark.read.jdbc(JDBC_URL, 'item', properties=DB_PROPERTIES)
    courier_agg = order_driver.groupBy("order_id").agg(F.count("driver_id").alias("d_count")) \
        .withColumn("is_courier_changed", F.when(F.col("d_count") > 1, 1).otherwise(0))

    items_agg = order_items.groupBy("order_id").agg(
        F.sum(F.col("item_price") * F.col("item_quantity") - F.col("item_discount")).alias("total_turnover"),
        F.sum(F.col("item_price") * (F.col("item_quantity") - F.col("item_canceled_quantity")) - F.col("item_discount")).alias("total_revenue"),
        F.countDistinct(F.when(F.col("item_replaced_id").isNotNull(), F.col("item_id"))).alias("replacements_count")
    )

    df_orders = orders.join(items_agg, "order_id", "left") \
                      .join(stores, "store_id", "left") \
                      .join(delivery, "order_id", "left") \
                      .join(courier_agg, "order_id", "left")

    final_orders = df_orders.withColumn("dt", F.to_date("created_at")) \
        .withColumn("city", F.split(F.col("store_address"), ",").getItem(0)) \
        .withColumn("profit", F.col("total_revenue") - F.coalesce(F.col("delivery_cost"), F.lit(0))) \
        .withColumn("is_delivered", F.when(F.col("delivered_at").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_canceled", F.when(F.col("canceled_at").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_service_error", F.when(F.col("order_cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой"), 1).otherwise(0))

    orders_datamart = final_orders.groupBy("dt", "city", "store_id").agg(
        F.sum("total_turnover").alias("turnover"),
        F.sum("total_revenue").alias("revenue"),
        F.sum("profit").alias("profit"),
        F.count("order_id").alias("orders_count"),
        F.sum("is_delivered").alias("delivered_orders_count"),
        F.sum("is_canceled").alias("canceled_orders_count"),
        F.sum("is_service_error").alias("service_errors_count"),
        F.sum(F.coalesce(F.col("is_courier_changed"), F.lit(0))).alias("courier_changes_count"),
        F.countDistinct("user_id").alias("unique_customers_count"),
        F.sum("replacements_count").alias("total_replacements")
    ).withColumn("avg_check", F.col("revenue") / F.col("orders_count")) \
     .withColumn("revenue_per_customer", F.col("revenue") / F.col("unique_customers_count"))

    df_products = order_items.join(orders, "order_id", "left") \
        .join(items_catalog, "item_id", "left") \
        .join(stores, "store_id", "left") \
        .withColumn("dt", F.to_date("created_at")) \
        .withColumn("city", F.split(F.col("store_address"), ",").getItem(0))

    product_daily = df_products.groupBy("dt", "city", "item_category", "item_title").agg(
        F.sum("item_quantity").alias("total_qty"),
        F.sum(F.col("item_price") * (F.col("item_quantity") - F.col("item_canceled_quantity"))).alias("product_revenue"),
        F.sum("item_canceled_quantity").alias("canceled_qty"),
        F.countDistinct(F.when(F.col("item_canceled_quantity") > 0, F.col("order_id"))).alias("orders_with_cancel_count")
    )

    win_city = Window.partitionBy("dt", "city").orderBy(F.desc("total_qty"))
    products_datamart = product_daily.withColumn("is_top_in_city_day", F.row_number().over(win_city) == 1)
    orders_datamart.write.jdbc(url=JDBC_URL, table="orders_datamart", mode="overwrite", properties=DB_PROPERTIES)
    products_datamart.write.jdbc(url=JDBC_URL, table="products_datamart", mode="overwrite", properties=DB_PROPERTIES)
    
    spark.stop()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='pyspark_metrics_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['final_project', 'pyspark']
) as dag:

    calculate_all = PythonOperator(
        task_id='calculate_all_marts',
        python_callable=run_full_analytics
    )