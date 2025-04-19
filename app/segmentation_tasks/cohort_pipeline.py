import pandas as pd
import numpy as np
from operator import attrgetter
import json
from datetime import datetime

# Загрузка данных
def load_data():
    customers = pd.read_csv("research/clean_data/customers.csv")
    geolocation = pd.read_csv("research/clean_data/geolocation.csv")
    order_pay = pd.read_csv("research/clean_data/order_payments.csv")
    reviews = pd.read_csv("research/clean_data/order_reviews.csv")
    orders = pd.read_csv("research/clean_data/orders.csv")
    item = pd.read_csv("research/clean_data/orders_items.csv")
    category_name = pd.read_csv("research/clean_data/product_category_name_translation.csv")
    products = pd.read_csv("research/clean_data/products.csv")
    sellers = pd.read_csv("research/clean_data/sellers.csv")
    return customers, geolocation, order_pay, reviews, orders, item, category_name, products, sellers

# Объединение данных
def merge_data(orders, item, order_pay, reviews, products, customers, sellers, category_name):
    df = orders.merge(item, on='order_id', how='left')
    df = df.merge(order_pay, on='order_id', how='outer', validate='m:m')
    df = df.merge(reviews, on='order_id', how='outer')
    df = df.merge(products, on='product_id', how='outer')
    df = df.merge(customers, on='customer_id', how='outer')
    df = df.merge(sellers, on='seller_id', how='outer')
    df = df.merge(category_name, on="product_category_name", how="left")
    return df

# Очистка данных
def filter_customers(df):
    return df[~df["customer_unique_id"].isna()]

# Главная функция пайплайна
def main_pipeline():
    customers, geolocation, order_pay, reviews, orders, item, category_name, products, sellers = load_data()
    df = merge_data(orders, item, order_pay, reviews, products, customers, sellers, category_name)
    df = filter_customers(df)
    return df

# Препроцессинг и расчет когорт

def data_preprocessing():
    data = main_pipeline()
    data.dropna(inplace=True)

    data['order_purchase_timestamp'] = pd.to_datetime(data['order_purchase_timestamp'])

    data['cohort_month'] = data.groupby('customer_unique_id')['order_purchase_timestamp'].transform('min').dt.to_period('M').astype(str)
    data['order_month'] = data['order_purchase_timestamp'].dt.to_period('M').astype(str)
    data['cohort_index'] = (
    pd.to_datetime(data['order_month']) - pd.to_datetime(data['cohort_month'])).dt.days // 30


    # Основные таблицы
    cohort_data = data.groupby(['cohort_month', 'cohort_index'])['customer_unique_id'].nunique().unstack(1)
    retention = cohort_data.copy()
    cohort_size = retention.iloc[:, 0]
    retention = retention.divide(cohort_size, axis=0)

    regional_cohort = data.groupby(['customer_state', 'cohort_month', 'cohort_index'])['customer_unique_id'].nunique().reset_index()
    state_list = data['customer_state'].dropna().unique().tolist()

    # Преобразование индексов/колонок
    retention.columns = retention.columns.astype(str)
    retention.index = retention.index.astype(str)
    cohort_data.columns = cohort_data.columns.astype(str)
    cohort_data.index = cohort_data.index.astype(str)
    regional_cohort.columns = regional_cohort.columns.astype(str)
    
    print("[DEBUG] regional_cohort.columns", regional_cohort.columns.tolist())
    for col in regional_cohort.columns:
        try:
            samples = regional_cohort[col].head(10)
            for i, val in enumerate(samples):
                if isinstance(val, (dict, list, pd.DataFrame, pd.Series)):
                    print(f"[WARNING] В колонке '{col}' на строке {i} — вложенный тип: {type(val)}")
        except Exception as e:
            print(f"[ERROR] Не удалось проверить колонку '{col}': {e}")


    # Финальный JSON-ответ
    return {
        "state_list": state_list,
        "retention": json.loads(retention.to_json(orient="split")),
        "cohort_data": json.loads(cohort_data.to_json(orient="split")),
        "regional_cohort": json.loads(regional_cohort.to_json(orient="split")),
        "updated_at": datetime.utcnow().isoformat()
    }