import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

#below is in-built marker
@pytest.mark.skip("work in progress")
def test_read_customers_df(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435

def test_read_orders_df(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

#below is custom marker named transformation and specified in pytest.ini file
@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

#below is custom marker named slow and specified in pytest.ini file
@pytest.mark.slow()
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

#below is custom marker named newconftest and specified in pytest.ini file
#the expected_results file is in-correct in data> test_result folder
@pytest.mark.newconftest()
def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()

#below is an in-build marker used to parametrize the arguments
@pytest.mark.parametrize(
    "entry1, count",
    [("CLOSED", 7556),
     ("PENDING_PAYMENT", 15030),
     ("COMPLETE", 22899)]
)

#below marker used the above inbuilt markers values
#it will run 3 times for 3 different parameters

@pytest.mark.latest()
def test_check_count_df(spark, entry1, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, entry1).count()
    assert filtered_count ==  count