# This is an example feature definition file

from datetime import timedelta

from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import String, Int32, Float32
import pandas as pd
import os

DATA_REPO_NAME="ml-data-mgmt"
PARQUET_FILE_PATH= f"/home/ubuntu/.kaggle/{DATA_REPO_NAME}/Telco-Customer-Churn.parquet"

customer = Entity(name="customer", join_keys=["customerID"])

data_df = pd.read_parquet(PARQUET_FILE_PATH)

if "event_timestamp" in data_df.columns:
    data_df = data_df.drop(columns=["event_timestamp"])

timestamps = pd.date_range(
    end=pd.Timestamp.now(), 
    periods=len(data_df), 
    freq='D').to_frame(name="event_timestamp", index=False)

data_df = pd.concat(objs=[data_df, timestamps], axis=1)

data_df.to_parquet(PARQUET_FILE_PATH)

customer_stats_source = FileSource(
    name="telco_customer_source",
    path=PARQUET_FILE_PATH,
    timestamp_field="event_timestamp"
)

customer_stats_fv = FeatureView(
    name="telco_customer_stats",
    entities=[customer],
    schema = [
        Field(name="customerID", dtype=String),
        Field(name="gender", dtype=String),
        Field(name="DeviceProtection", dtype=String),
        Field(name="TechSupport", dtype=String),
        Field(name="StreamingTV", dtype=String),
        Field(name="StreamingMovies", dtype=String),
        Field(name="Contract", dtype=String),
        Field(name="PaperlessBilling", dtype=String),
        Field(name="PaymentMethod", dtype=String),
        Field(name="MonthlyCharges", dtype=Float32),
        Field(name="TotalCharges", dtype=Float32),
        Field(name="TotalAddonServices", dtype=Float32),
        Field(name="TenureGroup", dtype=String),
        Field(name="AvgMonthlyUsage", dtype=Float32),
        Field(name="Churn", dtype=String),
    ],
    online=True,
    source=customer_stats_source,
    tags={"team": "telco_customer_churn_data"},
)