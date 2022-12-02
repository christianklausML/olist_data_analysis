import os
import pandas as pd
import fnmatch


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities
        csv_path = os.path.dirname(__file__).replace(__package__, 'data/csv')
        
        file_names = fnmatch.filter(os.listdir(csv_path), "*.csv")
        
        key_names = file_names.copy()
        key_names = [key_name.replace('_dataset.csv', '').replace('.csv', '').replace('olist_', '') for key_name in key_names]
        
        data = {}
        
        for key_name, file_name in zip(key_names, file_names):
            data[key_name] = pd.read_csv(os.path.join(csv_path, file_name))
          
        return data
        

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
