import os
from dotenv import load_dotenv

# from pathlib import Path
# from pyiceberg.catalog import load_catalog, Catalog
# from pyiceberg.schema import Schema, NestedField
# from pyiceberg.types import IntegerType, StringType

# Get env variables
load_dotenv()
# Create connection for AWS booto3
# ================================
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
S3_BUCKET = os.environ.get('S3_BUCKET')

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, FloatType, BooleanType

# Define the catalog configuration
catalog_config = {
    "type": "rest",
    "uri": "http://127.0.0.1:9001/iceberg",
    # "warehouse": "s3://lbr-files/GEX/DB",
    "s3.endpoint": "http://s3.eu-central-1.wasabisys.com",  # S3 endpoint URL
    "s3.access-key-id": '3JQWQCTIFWVBOHJFBVB2',
    "s3.secret-access-key": 'AVUmAIzJ8upmliyW5w03LBndXoelP7cWAu3zVeea',
    "s3.region": "eu-central-1",
    "s3.path-style-access": "true",
}

# Load the catalog
catalog = load_catalog("test", **catalog_config)


# Define the schema for the table
schema = Schema(
    NestedField(id=1, name="product_id", field_type=StringType(), required=True),
    NestedField(id=2, name="product", field_type=StringType(), required=True),
    NestedField(id=3, name="sales_price", field_type=FloatType(), required=True),
    NestedField(id=4, name="qt", field_type=IntegerType(), required=True),
    NestedField(id=5, name="available", field_type=BooleanType(), required=True),
)

# Define the table identifier
# table_identifier = "test.vegetables"
catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
    location="s3://lbr-files/GEX/FILES",
)
# Create the table
# catalog.create_table(table_identifier, schema)

print(f"Table created successfully.")
# try:
    
# except Exception as e:
#     print(f"Failed to create table {table_identifier}: {e}")