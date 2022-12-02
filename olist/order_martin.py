import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        orders = self.data['orders'].copy()
        if is_delivered:
            orders = orders[orders['order_status']=='delivered']
        else: orders = orders
        columns = ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date',
            'order_delivered_customer_date','order_estimated_delivery_date']
        for col in columns:
            orders[col] = pd.to_datetime(orders[col])

        orders['wait_time'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.days
        orders['expected_wait_time'] = (orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']).dt.days
        orders['delay_vs_expected'] = orders['wait_time'] - orders['expected_wait_time']
        orders['delay_vs_expected'] = orders['delay_vs_expected'].clip(lower=0)
        orders = orders[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected', 'order_status']]
        return orders

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews'].copy()
        reviews['dim_is_five_star'] = reviews['review_score'].apply(lambda x: 1 if x ==5 else 0)
        reviews['dim_is_one_star'] = reviews['review_score'].apply(lambda x: 1 if x ==1 else 0)
        reviews = reviews[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]
        return reviews

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        order_items = self.data['order_items'].copy()
        order_items = order_items[['order_id','product_id']].groupby('order_id').agg(
            number_of_products = pd.NamedAgg(column="product_id", aggfunc="count"))
        order_items.reset_index(inplace = True)
        return order_items

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_sellers = self.data['order_items'].copy()
        order_sellers = order_sellers[['order_id', 'seller_id']].groupby('order_id').agg(
            number_of_sellers = pd.NamedAgg(column="seller_id", aggfunc="nunique"))
        order_sellers.reset_index(inplace = True)
        order_sellers.describe()
        return order_sellers

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        price_freight = self.data['order_items'].copy()
        price_freight = price_freight[['order_id', 'price','freight_value']].groupby('order_id').agg(
            price = pd.NamedAgg(column="price", aggfunc="sum"),
            freight_value = pd.NamedAgg(column='freight_value', aggfunc='sum'))
        price_freight.reset_index(inplace = True)
        return price_freight

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        distance = self.data['orders'].copy()
        distance = distance[['order_id']]
        distance['distance_seller_customer'] = 581.0
        return distance

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above
        wait_time = self.get_wait_time()
        review_score = self.get_review_score()
        number_products = self.get_number_products()
        number_sellers = self.get_number_sellers()
        price_freight = self.get_price_and_freight()
        distance = self.get_distance_seller_customer()
        training_data = wait_time.merge(review_score, how = 'inner', on = 'order_id')
        training_data = training_data.merge(number_products, how = 'inner', on = 'order_id')
        training_data = training_data.merge(number_sellers, how = 'inner', on = 'order_id')
        training_data = training_data.merge(price_freight, how = 'inner', on = 'order_id')
        training_data = training_data.merge(distance, how = 'inner', on = 'order_id')        
        training_data.dropna(inplace = True)
        #training_data = training_data.merge(distance, how = 'inner', on = 'order_id')
        return training_data
