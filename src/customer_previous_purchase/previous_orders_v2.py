from datetime import datetime
from tools.dictionaries import sql
from tools.tools import get_mysql, put_mysql
from dotenv import load_dotenv, find_dotenv
import os

# fetch records
def fetch_records(creds):
    print(f"fetching records: {datetime.now()}")
    orders = get_mysql(creds, sql()["all_orders"])
    return orders

# previous customer order
def previous_customer_order(orders,cust_cols):
    print(f"finding previous customer orders: {datetime.now()}")

    cust_df = orders[cust_cols].drop_duplicates()
    cust_df.sort_values(by=cust_cols, inplace=True)
    cust_df["previous_customer_order_id"], cust_df["previous_customer_order_date"] = (
        cust_df.groupby("customer_id")["order_id"].shift(),
        cust_df.groupby("customer_id")["order_date"].shift(),
    )
    return cust_df

# previous brand order
def previous_brand_order(orders,brand_cols):
    print(f"finding previous brand orders: {datetime.now()}")

    brand_df = orders[brand_cols].drop_duplicates()
    brand_df.sort_values(by=brand_cols, inplace=True)
    brand_df["previous_brand_order_id"], brand_df["previous_brand_order_date"] = (
        brand_df.groupby(["customer_id", "brand_id"])["order_id"].shift(),
        brand_df.groupby(["customer_id", "brand_id"])["order_date"].shift(),
    )
    return brand_df

# previous product order
def previous_product_order(orders,prod_cols):
    print(f"finding previous product orders: {datetime.now()}")
    prod_df = orders[prod_cols].drop_duplicates()
    prod_df.sort_values(by=prod_cols, inplace=True)
    prod_df["previous_product_order_id"], prod_df["previous_product_order_date"] = (
        prod_df.groupby(["customer_id", "product_id"])["order_id"].shift(),
        prod_df.groupby(["customer_id", "product_id"])["order_date"].shift(),
    )
    return prod_df

#load environmental variables
load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host,user,passw,schema)

#merge variables
cust_cols = ["customer_id", "order_id", "order_date"]
brand_cols = ["customer_id", "brand_id", "order_date", "order_id"]
prod_cols = ["customer_id", "product_id", "order_date", "order_id"]

if __name__ == "__main__":
    orders = fetch_records(creds)
    cust_df = previous_customer_order(orders,cust_cols)
    brand_df = previous_brand_order(orders,brand_cols)
    prod_df = previous_product_order(orders,prod_cols)
    print(f"merging answers: {datetime.now()}")
    orders = orders.merge(cust_df, how="left", on=cust_cols)
    orders = orders.merge(brand_df, how="left", on=brand_cols)
    orders = orders.merge(prod_df, how="left", on=prod_cols)
    orders.fillna('\\N', inplace=True)

    print(f"inserting answers into database: {datetime.now()}")
    put_mysql(creds, orders, "previous_orders")
    print("Process completed.")