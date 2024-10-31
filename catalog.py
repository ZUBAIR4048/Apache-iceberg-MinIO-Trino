import pyarrow as pa
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, FloatType, DoubleType, NestedField
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Configure PostgreSQL catalog connection with MinIO warehouse path
catalog = SqlCatalog(
    "minio_catalog",
    **{
        "uri": "postgresql+psycopg2://postgres:12345678@localhost/postgres",
        "warehouse": "s3://bucket-test3",  # MinIO bucket URL
        "s3.endpoint": "http://127.0.0.1:9000",  # MinIO endpoint
        "s3.access-key": "nadu",
        "s3.secret-key": "12345678",
        "s3.region": "ap-northeast-1",
    },
)

# Define the schema for the table
schema = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=2, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=3, name="ask", field_type=DoubleType(), required=False)
)

# Define the sort order for the table
sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))

# Create namespace (if not already created) and table in MinIO bucket
catalog.create_namespace("docs_example7")
table = catalog.create_table(
    identifier="docs_example7.bids7",
    schema=schema,
    sort_order=sort_order,
    location="s3://bucket-test3/docs_example7/bids7"  # Store data in MinIO bucket
)

# Generate sample data as a DataFrame
sample_data = pd.DataFrame({
    "symbol": ["AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "try"],
    "bid": [150.5, 2800.2, 300.1, 900.0, 3400.7, 32.7],
    "ask": [151.0, 2801.5, 301.0, 901.5, 3402.1, 9.7],
})

# Convert DataFrame to PyArrow Table
arrow_table = pa.Table.from_pandas(
    sample_data,
    schema=pa.schema([
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("bid", pa.float32()),
        pa.field("ask", pa.float64())
    ])
)

# Append data to the Iceberg table
table.append(arrow_table)

# Confirm data insertion by printing table data
print(table.scan().to_arrow())

# Optionally, filter rows with a condition
df_filtered = table.scan(row_filter="bid > 100").to_arrow()
print(df_filtered)
