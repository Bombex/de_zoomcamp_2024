import pandas as pd
import requests

taxi_dtypes = {
    "VendorID": pd.Int64Dtype(),
    "passenger_count": pd.Int64Dtype(),
    "trip_distance": float,
    "RatecodeID": pd.Int64Dtype(),
    "store_and_fwd_flag": str,
    "PULocationID": pd.Int64Dtype(),
    "DOLocationID": pd.Int64Dtype(),
    "payment_type": pd.Int64Dtype(),
    "fare_amount": float,
    "extra": float,
    "mta_tax": float,
    "tip_amount": float,
    "tolls_amount": float,
    "improvement_surcharge": float,
    "total_amount": float,
    "congestion_surcharge": float,
    "trip_type": pd.Int64Dtype(),
}

parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

months = list(range(1, 13))

df = pd.DataFrame()


for month in months:
    if month < 10:
        month = f"0{month}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError("link is unavailable")
    new_df = pd.read_parquet(url, engine="pyarrow")
    new_df = new_df.astype(taxi_dtypes)
    for date_col in parse_dates:
        new_df[date_col] = pd.to_datetime(new_df[date_col])
    df = pd.concat([df, new_df], ignore_index=True)
    print("DataFrame shape: ", df.shape)
