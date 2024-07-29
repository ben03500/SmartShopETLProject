"""
File name: main.py
Author: Pattarapong Danpoonkij
Date created: 2024-07-28
Date Edited: -
Python Version: 3.12.4
"""

import logging

import pandas as pd
import sqlalchemy as sa

import config as cfg

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter('%(asctime)s %(name)-20s %(levelname)-8s %(message)s'))

logging.getLogger().addHandler(stream_handler)

logger = logging.getLogger()

logger.setLevel(logging.INFO)


class DataFactory:
    @staticmethod
    def customer_transaction(filepath) -> pd.DataFrame:
        """
            Read customer transaction data from a csv file
        """
        logger.info(f"Reading the source CSV file.")
        try:
            df = pd.read_json(filepath)
            df.timestamp = pd.to_datetime(df['timestamp'])
            return df
        except IOError as e:
            print(f"Error reading the customer transaction file: {e}")

    @staticmethod
    def product_catalog(filepath) -> pd.DataFrame:
        """
            Read product catalog data from a json file
        """
        logger.info(f"Reading the source JSON file.")
        try:
            return pd.read_csv(filepath)
        except IOError as e:
            print(f"Error reading the product catalog file: {e}")


class ETLOrchestrator:
    def __init__(self, filepath_dict, database_connection_url) -> None:
        self._data = DataFactory()
        self.filepath_dict = filepath_dict
        self.connection_url = database_connection_url

    @property
    def customer_transaction_df(self) -> pd.DataFrame:
        """
            Retrieve the customer transactions data
        """
        return self._data.customer_transaction(self.filepath_dict["transaction"])

    @property
    def product_catalog_df(self) -> pd.DataFrame:
        """
            Retrieve and clean the product catalog detail from data/product_catalog.csv file
        """
        df = self._data.product_catalog(self.filepath_dict["product"])
        # check and drop duplicated products based on their IDs
        duplicate_count = df.duplicated(subset='product_id').sum()
        if duplicate_count > 0:
            logger.warning(f"Number of duplicate product IDs: {duplicate_count}")
        df = df.drop_duplicates(subset='product_id', keep='first')
        # check and nullify the prices that are negatives or could not been converted to numbers
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        invalid_price_count = df['price'].isna().sum() + (df['price'] < 0).sum()
        if invalid_price_count > 0:
            logger.warning(f"Number of invalid prices: {invalid_price_count}")
        df['price'] = df['price'].apply(lambda x: x if x >= 0 else None)
        # check and putting a placeholder for the missing product name
        missing_product_name_count = df['product_name'].isna().sum()
        if missing_product_name_count > 0:
            logger.warning(f"Number of missing product names: {missing_product_name_count}")
        df['product_name'] = df['product_name'].fillna('Unknown Product')
        return df

    @property
    def transaction_product_joined_df(self) -> pd.DataFrame:
        df = self.product_catalog_df
        # outer join product catalog and customer transaction data
        merged_df = df.merge(self.customer_transaction_df, how='outer', on='product_id')
        # get the product price from customer transaction or product catalog if one or the other is unavailable
        merged_df['price'] = merged_df['price_x'].combine_first(merged_df['price_y'])
        return merged_df

    @property
    def dim_customer_df(self) -> pd.DataFrame:
        return self.customer_transaction_df[['customer_id']].drop_duplicates().reset_index(drop=True)

    @property
    def dim_product_df(self):
        df = self.transaction_product_joined_df[
            ["product_id", "product_name", "category", "price"]].sort_values(by="price", ascending=False)
        df = df.drop_duplicates(subset=["product_id", "product_name", "category"]).reset_index(drop=True)
        return df

    @property
    def dim_time_df(self):
        # extract the relevant timestamps from the transaction data
        df = self.customer_transaction_df[['timestamp']]
        # generate a complete date range from the minimum to the maximum date
        min_dt = df['timestamp'].min()
        max_dt = df['timestamp'].max()
        dt_range = pd.date_range(start=min_dt, end=max_dt)
        # create a dataframe from the date range
        date_df = pd.DataFrame(dt_range, columns=['timestamp'])
        date_df['date'] = date_df['timestamp'].dt.date
        date_df = date_df.drop_duplicates(subset="date", keep="first")
        date_df['year'] = date_df['timestamp'].dt.year
        date_df['month'] = date_df['timestamp'].dt.month
        date_df['day'] = date_df['timestamp'].dt.day
        date_df = date_df.drop(columns=['timestamp'])
        return date_df

    @property
    def fact_sale_df(self):
        df = self.customer_transaction_df.copy()
        df['date'] = df['timestamp'].dt.date
        df = df.drop(columns=['timestamp'])
        df['total_sales'] = df['quantity'] * df['price']
        grouped_df = df.groupby(['date', 'transaction_id', 'customer_id', 'product_id']).agg({
            'price': 'first',
            'quantity': 'sum',
            'total_sales': 'sum'
        }).reset_index()
        return grouped_df

    def load(self):
        try:
            logger.info(f"Loading data to the database.")
            engine = sa.create_engine(self.connection_url)
            with engine.connect() as connection:
                self.dim_customer_df.to_sql("customer", con=connection, if_exists="replace", index=False)
                self.dim_product_df.to_sql("product", con=connection, if_exists="replace", index=False)
                self.dim_time_df.to_sql("time", con=connection, if_exists="replace", index=False)
                self.fact_sale_df.to_sql("sale", con=connection, if_exists="replace", index=False)
            logger.info(f"Loaded data to the database.")
        except Exception as e:
            logger.error(f"Error during the loading process: {e}")


def main():
    orchestrator = ETLOrchestrator(cfg.filepaths, cfg.connection_url)
    logger.info(f"Initiating the ShopSmart ETL process.")
    try:
        orchestrator.load()
        logger.info(f"Completed the ShopSmart ETL process.")
    except Exception as e:
        logger.error(f"Error during the ETL process: {e}")
        raise


if __name__ == "__main__":
    main()
