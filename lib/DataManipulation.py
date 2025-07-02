from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")

def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df,"customer_id")


def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

# generic function created to parametrize the order status instead of hardcoding
def filter_orders_generic(orders_df, status):
    return orders_df.filter("order_status='{}'".format(status))