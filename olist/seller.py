
import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order


class Seller:
    def __init__(self):
        # Import data only once
        olist = Olist()
        self.data = olist.get_data()
        self.order = Order()

    def get_seller_features(self):
        """
        Returns a DataFrame with:
        'seller_id', 'seller_city', 'seller_state'
        """
        sellers = self.data['sellers'].copy(
        )  # Make a copy before using inplace=True so as to avoid modifying self.data
        sellers.drop('seller_zip_code_prefix', axis=1, inplace=True)
        sellers.drop_duplicates(
            inplace=True)  # There can be multiple rows per seller
        return sellers

    def get_seller_delay_wait_time(self):
        """
        Returns a DataFrame with:
        'seller_id', 'delay_to_carrier', 'wait_time'
        """
        # Get data
        order_items = self.data['order_items'].copy()
        orders = self.data['orders'].query("order_status=='delivered'").copy()

        ship = order_items.merge(orders, on='order_id')

        # Handle datetime
        ship.loc[:, 'shipping_limit_date'] = pd.to_datetime(
            ship['shipping_limit_date'])
        ship.loc[:, 'order_delivered_carrier_date'] = pd.to_datetime(
            ship['order_delivered_carrier_date'])
        ship.loc[:, 'order_delivered_customer_date'] = pd.to_datetime(
            ship['order_delivered_customer_date'])
        ship.loc[:, 'order_purchase_timestamp'] = pd.to_datetime(
            ship['order_purchase_timestamp'])

        # Compute delay and wait_time
        def delay_to_logistic_partner(d):
            days = np.mean(
                (d.order_delivered_carrier_date - d.shipping_limit_date) /
                np.timedelta64(24, 'h'))
            if days > 0:
                return days
            else:
                return 0

        def order_wait_time(d):
            days = np.mean(
                (d.order_delivered_customer_date - d.order_purchase_timestamp)
                / np.timedelta64(24, 'h'))
            return days

        delay = ship.groupby('seller_id')\
                    .apply(delay_to_logistic_partner)\
                    .reset_index()
        delay.columns = ['seller_id', 'delay_to_carrier']

        wait = ship.groupby('seller_id')\
                   .apply(order_wait_time)\
                   .reset_index()
        wait.columns = ['seller_id', 'wait_time']

        df = delay.merge(wait, on='seller_id')

        return df

    def get_active_dates(self):
        """
        Returns a DataFrame with:
        'seller_id', 'date_first_sale', 'date_last_sale', 'months_on_olist'
        """
        # First, get only orders that are approved
        orders_approved = self.data['orders'][[
            'order_id', 'order_approved_at'
        ]].dropna()

        # Then, create a (orders <> sellers) join table because a seller can appear multiple times in the same order
        orders_sellers = orders_approved.merge(self.data['order_items'],
                                               on='order_id')[[
                                                   'order_id', 'seller_id',
                                                   'order_approved_at'
                                               ]].drop_duplicates()
        orders_sellers["order_approved_at"] = pd.to_datetime(
            orders_sellers["order_approved_at"])

        # Compute dates
        orders_sellers["date_first_sale"] = orders_sellers["order_approved_at"]
        orders_sellers["date_last_sale"] = orders_sellers["order_approved_at"]
        df = orders_sellers.groupby('seller_id').agg({
            "date_first_sale": min,
            "date_last_sale": max
        })
        df['months_on_olist'] = round(
            (df['date_last_sale'] - df['date_first_sale']) /
            np.timedelta64(1, 'M'))
        return df

    def get_quantity(self):
        """
        Returns a DataFrame with:
        'seller_id', 'n_orders', 'quantity', 'quantity_per_order'
        """
        order_items = self.data['order_items']

        n_orders = order_items.groupby('seller_id')['order_id']\
            .nunique()\
            .reset_index()
        n_orders.columns = ['seller_id', 'n_orders']

        quantity = order_items.groupby('seller_id', as_index=False).agg(
            {'order_id': 'count'})
        quantity.columns = ['seller_id', 'quantity']

        result = n_orders.merge(quantity, on='seller_id')
        result['quantity_per_order'] = result['quantity'] / result['n_orders']
        return result

    def get_sales(self):
        """
        Returns a DataFrame with:
        'seller_id', 'sales'
        """
        return self.data['order_items'][['seller_id', 'price']]\
            .groupby('seller_id')\
            .sum()\
            .rename(columns={'price': 'sales'})

    def get_review_score(self):
        """
        Returns a DataFrame with:
        'seller_id', 'share_of_five_stars', 'share_of_one_stars', 'review_score'
        """
        temp = pd.merge(left=self.data['order_items'], right=self.data['order_reviews'], on="order_id").drop([
                        "order_id",
                        "order_item_id",
                        "product_id",
                        "shipping_limit_date",
                        "price",
                        "freight_value",
                        "review_id",
                        "review_comment_title",
                        "review_comment_message",
                        "review_creation_date",
                        "review_answer_timestamp"], axis=1)

        temp["share_of_five_stars"] = temp["review_score"] == 5
        temp["share_of_one_stars"] = temp["review_score"] == 1
        temp["number"] = temp["review_score"]

        review_score = temp.groupby("seller_id").sum()
        review_score["number"] = temp.groupby("seller_id").agg({"number": "count"})
        review_score["review_score"] = temp.groupby("seller_id")["review_score"].mean()
        review_score["share_of_five_stars"] = temp.groupby("seller_id")["share_of_five_stars"].sum() / review_score["number"]
        review_score["share_of_one_stars"] = temp.groupby("seller_id")["share_of_one_stars"].sum() / review_score["number"]

        review_score = review_score.drop("number", axis=1).reset_index()
        review_score = review_score[['seller_id', 'share_of_five_stars', 'share_of_one_stars', 'review_score']]

        return review_score


    def get_revenue_cost(self):
        """
        Returns a DataFrame with:
        ['seller_id', 'revenue', 'total_review_cost', 'profits']
        """

        def review_cost(score):
            cost = 0
            if score == 1:
                cost = 100
            elif score == 2:
                cost = 50
            elif score == 3:
                cost = 40
            return cost

        monthly_charge = 80 # monthly charge

        cost = self.data['orders'][['order_id']].dropna().merge(
                    self.data['order_reviews'][["order_id", "review_score"]], on="order_id").merge(
                        self.data['order_items'][["order_id", "seller_id"]], on="order_id")
        cost["review_cost"] = cost["review_score"].agg(review_cost)

        cost = cost.groupby("seller_id").sum().drop("review_score", axis=1)

        cost["revenue"] = round(self.get_active_dates()["months_on_olist"] * monthly_charge\
            + self.get_sales()["sales"] * 0.10, 2) # 10% cut from the sales
        cost = cost.reset_index()
        cost = cost[['seller_id', 'revenue', 'review_cost']]

        # Calculating the profits
        cost["profits"] = cost["revenue"] - cost["review_cost"]

        return cost

    def get_training_data(self):
        """
        Returns a DataFrame with:
        ['seller_id', 'seller_city', 'seller_state', 'delay_to_carrier',
        'wait_time', 'date_first_sale', 'date_last_sale', 'months_on_olist', 'share_of_one_stars',
        'share_of_five_stars', 'review_score', 'n_orders', 'quantity',
        'quantity_per_order', 'sales', 'revenue', 'total_review_cost', 'profits']
        """

        training_set =\
            self.get_seller_features()\
                .merge(
                self.get_seller_delay_wait_time(), on='seller_id'
               ).merge(
                self.get_active_dates(), on='seller_id'
               ).merge(
                self.get_quantity(), on='seller_id'
               ).merge(
                self.get_sales(), on='seller_id'
               )

        if self.get_review_score() is not None:
            training_set = training_set.merge(
                self.get_review_score(), on='seller_id').merge(
                    self.get_revenue_cost(), on='seller_id')

        return training_set


def main():
    Seller().get_review_score()

if __name__ == "__main__":
    main()
