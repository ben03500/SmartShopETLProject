"""
File name: config.py
Author: Pattarapong Danpoonkij
Date created: 2024-07-28
Date edited: -
Python Version: 3.12.4
"""

import os
from pathlib import Path

# File paths for the input data
filepaths = {"transaction": Path.cwd() / "data/customer_transactions.json",
             "product": Path.cwd() / "data/product_catalog.csv"}

# Load database configuration from environment variables (no input need from users)
DB_PROTOCOL = os.getenv('DB_PROTOCOL')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

connection_url = f'{DB_PROTOCOL}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
