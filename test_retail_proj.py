import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders,filter_closed_orders_generic
from lib.ConfigReader import get_app_config
from lib.DataManipulation import count_orders_state

@pytest.mark.skip
def test_read_customers_df(spark):
    customers_count=read_customers(spark,"LOCAL").count()
    print("Customers count is",customers_count)
    assert customers_count==12435
@pytest.mark.skip
def test_read_orders_df(spark):
    orders_count=read_orders(spark,"LOCAL").count()
    print("Orders count is ",orders_count)
    assert orders_count==68884
@pytest.mark.skip
def test_filter_closed_orders(spark):
    orders_count=read_orders(spark,"LOCAL")
    filter_closed_orders_count=filter_closed_orders(orders_count).count()
    assert filter_closed_orders_count==7556
@pytest.mark.skip
def test_read_app_config():
    config= get_app_config("LOCAL")
    assert config["orders.file.path"]=="data/orders.csv"
@pytest.mark.skip
def test_count_orders_state(spark,expected_results):
    customers_df=read_customers(spark,"LOCAL")
    actual_results=count_orders_state(customers_df)
    assert actual_results.collect()==expected_results.collect()
@pytest.mark.skip
def test_filter_closed_orders_generic(spark):
    orders_count=read_orders(spark,"LOCAL")
    filter_closed_orders_count=filter_closed_orders_generic(orders_count,"CLOSED").count()
    assert filter_closed_orders_count==7556

@pytest.mark.parametrize("status,count",[("CLOSED",7556),("PENDING_PAYMENT",15030),("COMPLETE",22900)])
def test_filter_closed_orders(spark,status,count):
    orders_count=read_orders(spark,"LOCAL")
    filter_closed_orders_count=filter_closed_orders_generic(orders_count,status).count()
    assert filter_closed_orders_count==count