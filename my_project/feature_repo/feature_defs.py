
from datetime import timedelta
import os
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String

DATA_REPO_NAME = 'ml-data-mgmt'
PARQET_FILE = f'/home/ubuntu/.kaggle/{DATA_REPO_NAME}/Telco-Customer-Churn.parquet'

# Define an entity for the driver -  entity as a primary key used to
# fetch features.
customerid = Entity(name="customer", join_keys=["customerID"])

df = pd.read_parquet(PARQET_FILE)

if 'event_timestamp' in df.columns:
    df = df.drop(columns=["event_timestamp"])

# Add artificail timestamps
ts = pd.date_range(
    end = pd.Timestamp.now(),
    periods = len(df),
    freq = 'D').to_frame(name="event_ts", index=False)

df = pd.concat(objs = [df, ts], axis=1)

df.to_parquet(PARQET_FILE)

customer_stats_file_src = FileSource(
    name='telco_cus_src',
    path = PARQET_FILE,
    timestamp_field="event_ts"
)

customer_stats_fv = FeatureView(
    name='telco_cust_stats',
    entities = [customerid],
    schema = [
        Field(name='customerId', dtype=String),
        Field(name='TotalCharges', dtype=Float32),
    ],
    online=True,
    source=customer_stats_file_src,
    tags={"team": "telco_cc_data"},
)
