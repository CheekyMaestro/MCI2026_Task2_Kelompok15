
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob
import math


def clean_for_clickhouse(df, nullable_cols=None):
    
    if nullable_cols is None:
        nullable_cols = []

    for col in df.columns:
        if col in nullable_cols:
            df[col] = df[col].where(df[col].notna(), None)
            # Ganti inf/-inf juga
            df[col] = df[col].apply(
                lambda x: None if x is not None and isinstance(x, float)
                and (math.isinf(x) or math.isnan(x)) else x
            )
    return df

# STEP 1: Inisialisasi Spark
def run_orders_analytics():
  
    
    
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Pipeline") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print(" Membaca data orders dari Data Lake...")

    try:
        df_orders = spark.read.parquet(
            "file:///opt/airflow/data_lake/orders/raw_orders/"
        )
        df_products = spark.read.parquet(
            "file:///opt/airflow/data_lake/orders/raw_products/"
        )
    except Exception as e:
        print(f" Gagal membaca data parquet: {e}")
        spark.stop()
        raise

    orders_count = df_orders.count()
    products_count = df_products.count()
    print(f"Data terbaca: {orders_count} orders, {products_count} products")

    
    # STEP 2: Agregasi dengan PySpark
    
    print(" Menghitung agregasi...")

    # Agregasi 1: Top 30 Produk Terlaris 
    top_products = df_products.groupBy("product_id", "product_name") \
        .agg(
            F.count("*").alias("order_count"),
            F.sum("reordered").alias("reorder_count")
        ) \
        .withColumn("reorder_rate",
                     F.round(F.col("reorder_count") / F.col("order_count") * 100, 2)) \
        .orderBy(F.desc("order_count")) \
        .limit(30)

    # Agregasi 2: Statistik per Department 
    dept_stats = df_products.groupBy("department_id", "department") \
        .agg(
            F.count("*").alias("total_products_ordered"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum("reordered").alias("reorder_count"),
            F.countDistinct("order_id").alias("order_count")
        ) \
        .withColumn("reorder_rate",
                     F.round(F.col("reorder_count") /
                             F.col("total_products_ordered") * 100, 2)) \
        .orderBy(F.desc("total_products_ordered"))

    # Agregasi 3: Top 30 Aisle Terpopuler 
    aisle_stats = df_products.groupBy("aisle_id", "aisle", "department") \
        .agg(
            F.count("*").alias("total_products_ordered"),
            F.countDistinct("product_id").alias("unique_products")
        ) \
        .orderBy(F.desc("total_products_ordered")) \
        .limit(30)

    # Agregasi 4: Distribusi Order per Jam 
    hourly_dist = df_orders.groupBy("order_hour_of_day") \
        .agg(F.count("*").alias("order_count")) \
        .orderBy("order_hour_of_day")

    #  Agregasi 5: Distribusi Order per Hari 
    dow_dist = df_orders.groupBy("order_dow") \
        .agg(F.count("*").alias("order_count")) \
        .orderBy("order_dow")

  
    # STEP 3: Konversi ke Pandas
   
    print(" Konversi hasil ke Pandas...")
    orders_pd = df_orders.toPandas()
    products_pd = df_products.toPandas()
    top_products_pd = top_products.toPandas()
    dept_stats_pd = dept_stats.toPandas()
    aisle_stats_pd = aisle_stats.toPandas()
    hourly_pd = hourly_dist.toPandas()
    dow_pd = dow_dist.toPandas()

    spark.stop()

    # Bersihkan NaN untuk kolom nullable
    orders_pd = clean_for_clickhouse(orders_pd,
                                     nullable_cols=['days_since_prior_order'])

   
    # STEP 4: Load ke ClickHouse
    
    print(" Memuat data ke ClickHouse...")

    client = Client(
        host='clickhouse-server',
        user='admin',
        password='rahasia'
    )

    # Buat Database 
    client.execute('CREATE DATABASE IF NOT EXISTS orders_analytics')

    #  Tabel 1: Raw Orders 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.orders (
            order_id UInt32,
            user_id UInt32,
            order_number UInt16,
            order_dow UInt8,
            order_hour_of_day UInt8,
            days_since_prior_order Nullable(Float32),
            eval_set String,
            product_count UInt16
        ) ENGINE = MergeTree()
        ORDER BY order_id
    ''')

    # Tabel 2: Raw Order Products 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.order_products (
            order_id UInt32,
            product_id UInt32,
            product_name String,
            aisle_id UInt16,
            aisle String,
            department_id UInt16,
            department String,
            add_to_cart_order UInt16,
            reordered UInt8
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
    ''')

    #  Tabel 3: Top Products 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.top_products (
            product_id UInt32,
            product_name String,
            order_count UInt32,
            reorder_count UInt32,
            reorder_rate Float32
        ) ENGINE = MergeTree()
        ORDER BY order_count
    ''')

    # Tabel 4: Department Stats 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.department_stats (
            department_id UInt16,
            department String,
            total_products_ordered UInt32,
            unique_products UInt32,
            reorder_count UInt32,
            order_count UInt32,
            reorder_rate Float32
        ) ENGINE = MergeTree()
        ORDER BY total_products_ordered
    ''')

    # Tabel 5: Aisle Stats 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.aisle_stats (
            aisle_id UInt16,
            aisle String,
            department String,
            total_products_ordered UInt32,
            unique_products UInt32
        ) ENGINE = MergeTree()
        ORDER BY total_products_ordered
    ''')

    #Tabel 6: Hourly Distribution 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.hourly_distribution (
            order_hour_of_day UInt8,
            order_count UInt32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    ''')

    # Tabel 7: Day of Week Distribution 
    client.execute('''
        CREATE TABLE IF NOT EXISTS orders_analytics.dow_distribution (
            order_dow UInt8,
            order_count UInt32
        ) ENGINE = MergeTree()
        ORDER BY order_dow
    ''')

  
    # STEP 5: Truncate & Insert (agar dashboard selalu fresh)
   
    def truncate_and_insert(table_name, df):
        """Helper: truncate tabel lalu insert data baru."""
        client.execute(f'TRUNCATE TABLE orders_analytics.{table_name}')
        data_tuples = [tuple(x) for x in df.to_numpy()]
        if data_tuples:
            client.execute(
                f'INSERT INTO orders_analytics.{table_name} VALUES',
                data_tuples
            )
        print(f"   {table_name}: {len(data_tuples)} baris dimuat")

    truncate_and_insert('orders', orders_pd)
    truncate_and_insert('order_products', products_pd)
    truncate_and_insert('top_products', top_products_pd)
    truncate_and_insert('department_stats', dept_stats_pd)
    truncate_and_insert('aisle_stats', aisle_stats_pd)
    truncate_and_insert('hourly_distribution', hourly_pd)
    truncate_and_insert('dow_distribution', dow_pd)

    
    # STEP 6: Cleanup parquet files
  
    print(" Membersihkan file parquet lama...")
    for subdir in ['raw_orders', 'raw_products']:
        pattern = f'/opt/airflow/data_lake/orders/{subdir}/*.parquet'
        for f in glob.glob(pattern):
            try:
                os.remove(f)
            except OSError as e:
                print(f"   Error menghapus {f}: {e.strerror}")

    print(" Pipeline Orders selesai!")


if __name__ == "__main__":
    run_orders_analytics()
