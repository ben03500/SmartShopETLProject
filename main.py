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

logger = logging.getLogger()


class DataFactory:
    @staticmethod
    def customer_transaction(filepath):
        try:
            df = pd.read_json(filepath)
            df.timestamp = pd.to_datetime(df['timestamp'])
            return df
        except IOError as e:
            print(f"Error reading the customer transaction file: {e}")

    @staticmethod
    def product_catalog(filepath):
        try:
            return pd.read_csv(filepath)
        except IOError as e:
            print(f"Error reading the product catalog file: {e}")


class ETLOrchestrator:
    def __init__(self, filepath_dict, database_connection_url):
        self.filepath_dict = filepath_dict
        self.connection_url = database_connection_url
        self._data = DataFactory()

    @property
    def customer_transaction_df(self) -> pd.DataFrame:
        return self._data.customer_transaction(self.filepath_dict["transaction"])

    @property
    def product_catalog_df(self) -> pd.DataFrame:
        df = self._data.product_catalog(self.filepath_dict["product"])
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        duplicate_count = df.duplicated(subset='product_id').sum()
        if duplicate_count > 0:
            logger.warning(f"Number of duplicate product IDs: {duplicate_count}")
        df = df.drop_duplicates(subset='product_id', keep='first')
        invalid_price_count = df['price'].isna().sum() + (df['price'] < 0).sum()
        if invalid_price_count > 0:
            logger.warning(f"Number of invalid prices: {invalid_price_count}")
        df['price'] = df['price'].apply(lambda x: x if x >= 0 else None)
        missing_product_name_count = df['product_name'].isna().sum()
        if missing_product_name_count > 0:
            logger.warning(f"Number of missing product names: {missing_product_name_count}")
        df['product_name'] = df['product_name'].fillna('Unknown Product')
        return df

    @property
    def transaction_product_joined_df(self) -> pd.DataFrame:
        df = self.product_catalog_df
        merged_df = df.merge(self.customer_transaction_df, how='outer', on='product_id')
        merged_df['price'] = merged_df['price_x'].combine_first(merged_df['price_y'])
        return merged_df

    @property
    def dim_customer_df(self):
        return self.customer_transaction_df[['customer_id']].drop_duplicates().reset_index(drop=True)

    @property
    def dim_product_df(self):
        df = self.transaction_product_joined_df[
            ["product_id", "product_name", "category", "price"]].sort_values(by="price", ascending=False)
        df = df.drop_duplicates(subset=["product_id", "product_name", "category"]).reset_index(drop=True)
        return df

    @property
    def dim_time_df(self):
        df = self.customer_transaction_df[['timestamp']]
        df['date'] = df['timestamp'].dt.date
        df = df.drop_duplicates(subset='date', keep='first')
        df['year'] = df['timestamp'].dt.year
        df['quarter'] = df['timestamp'].dt.to_period('Q').astype(str)
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df = df.drop(columns=['timestamp'])
        return df

    @property
    def fact_sale_df(self):
        df = self.customer_transaction_df.copy()
        df['date'] = df['timestamp'].dt.date
        df = df.drop(columns=['timestamp'])
        df['total_sales'] = df['quantity'] * df['price']
        return df

    def load(self):
        try:
            logger.info(f"Loading data to the database.")
            engine = sa.create_engine(self.connection_url)
            with engine.connect() as connection:
                self.dim_customer_df.to_sql("customer", con=connection, if_exists="replace", index=False)
                self.dim_product_df.to_sql("product", con=connection, if_exists="replace", index=False)
                self.dim_time_df.to_sql("time", con=connection, if_exists="replace", index=False)
                self.fact_sale_df.to_sql("sale", con=connection, if_exists="replace", index=False)
        except Exception as e:
            logger.error(f"Error during the loading process: {e}")


def main():
    orchestrator = ETLOrchestrator(cfg.filepaths, cfg.connection_url)
    logger.info(f"Initiating the ShopSmart ETL process.")
    try:
        orchestrator.load()
    except Exception as e:
        logger.error(f"Error during the ETL process: {e}")
        raise


if __name__ == "__main__":
    main()
