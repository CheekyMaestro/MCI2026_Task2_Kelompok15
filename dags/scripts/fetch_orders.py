import requests
import pandas as pd
import os
from datetime import datetime


def fetch_orders():

    print(" Mengambil data orders dari API...")
    url = "http://96.9.212.102:8000/orders"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        orders_list = data.get('orders', [])
        total = data.get('total_orders', 0)
        print(f" Menerima {total} orders dari API")


        # Flatten nested JSON menjadi 2 struktur tabular
 
        orders_data = []
        products_data = []

        for order in orders_list:
            # Order-level 
            orders_data.append({
                'order_id': order['order_id'],
                'user_id': order['user_id'],
                'order_number': order['order_number'],
                'order_dow': order['order_dow'],
                'order_hour_of_day': order['order_hour_of_day'],
                'days_since_prior_order': order.get('days_since_prior_order'),
                'eval_set': order.get('eval_set', 'unknown'),
                'product_count': len(order.get('products', []))
            })

            #Product-level (satu baris per produk per order)
            for product in order.get('products', []):
                products_data.append({
                    'order_id': order['order_id'],
                    'product_id': product['product_id'],
                    'product_name': product['product_name'],
                    'aisle_id': product['aisle_id'],
                    'aisle': product['aisle'],
                    'department_id': product['department_id'],
                    'department': product['department'],
                    'add_to_cart_order': product['add_to_cart_order'],
                    'reordered': product['reordered']
                })

        # Buat DataFrame
        df_orders = pd.DataFrame(orders_data)
        df_products = pd.DataFrame(products_data)

       
        # Simpan ke Data Lake sebagai Parquet
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Direktori terpisah untuk orders & products (beda schema)
        orders_dir = '/opt/airflow/data_lake/orders/raw_orders'
        products_dir = '/opt/airflow/data_lake/orders/raw_products'
        os.makedirs(orders_dir, exist_ok=True)
        os.makedirs(products_dir, exist_ok=True)

        orders_path = f'{orders_dir}/orders_{current_time}.parquet'
        products_path = f'{products_dir}/products_{current_time}.parquet'

        df_orders.to_parquet(orders_path, index=False)
        df_products.to_parquet(products_path, index=False)

        print(f" Tersimpan {len(df_orders)} orders → {orders_path}")
        print(f" Tersimpan {len(df_products)} products → {products_path}")

    except requests.exceptions.RequestException as e:
        print(f" Gagal mengambil data dari API: {e}")
        raise
    except Exception as e:
        print(f" Error tidak terduga: {e}")
        raise


if __name__ == "__main__":
    fetch_orders()
