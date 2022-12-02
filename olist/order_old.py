import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist
from functools import reduce


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
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        wait_time = self.data['orders'].copy()
        
        if is_delivered == True:
            wait_time = wait_time[wait_time['order_status']=='delivered']
        
        for col in ['order_purchase_timestamp', 'order_approved_at', \
            'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']: 
            wait_time[col] = pd.to_datetime(wait_time[col])
                    
        wait_time['wait_time'] = (wait_time['order_delivered_carrier_date'] - wait_time['order_approved_at']) / pd.to_timedelta(1, unit='D')
        wait_time['expected_wait_time'] = (wait_time['order_estimated_delivery_date'] - wait_time['order_approved_at']) / pd.to_timedelta(1, unit='D')
        wait_time['delay_vs_expected'] = wait_time['wait_time'] - wait_time['expected_wait_time']
        wait_time['delay_vs_expected'] = wait_time['delay_vs_expected'].apply(lambda x: x if x > 0 else 0)
        wait_time.drop(columns=['customer_id', 'order_purchase_timestamp',	'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'], inplace=True)
        
        return wait_time

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        review_score = self.data['order_reviews'].copy()
        
        review_score['dim_is_five_star'] = review_score['review_score'].apply(lambda x: 1 if x == 5 else 0)
        review_score['dim_is_one_star'] = review_score['review_score'].apply(lambda x: 1 if x == 1 else 0)
        review_score = review_score[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]
        
        return review_score

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        order_items = self.data['order_items'].copy()
        
        number_of_products = order_items[['order_id', 'product_id']].groupby(by='order_id').count()
        number_of_products.reset_index(inplace = True)
        number_of_products.rename(columns = {'product_id': 'number_of_products'}, inplace=True)
        
        return number_of_products

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        number_sellers = self.data['order_items'].copy()
        
        number_sellers = number_sellers[['order_id', 'seller_id']].groupby(by='order_id').nunique()
        number_sellers.reset_index(inplace = True)
        number_sellers.rename(columns = {'seller_id': 'number_of_sellers'}, inplace=True)
        
        return number_sellers

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
        pass  # YOUR CODE HERE

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
        #Hint: make sure to re-use your instance methods defined above
        data_frames = [Order().get_wait_time(), Order().get_review_score(), Order().get_number_products(), Order().get_number_sellers(), Order().get_price_and_freight()]
        training_data = reduce(lambda  left,right: pd.merge(left,right,on=['order_id'],
                                            how='inner'), data_frames)
        
        return training_data.dropna()
    