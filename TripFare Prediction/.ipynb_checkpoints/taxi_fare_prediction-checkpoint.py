import streamlit as st
import pandas as pd
import numpy as np
import joblib
import geohash2
from datetime import datetime

def get_distance_by_haversine(pick_lat, pick_lon, drop_lat, drop_lon):
    earth_radius = 6371
    pick_lat, pick_lon, drop_lat, drop_lon = map(np.radians,[pick_lat, pick_lon, drop_lat, drop_lon])
    lat = drop_lat - pick_lat
    lon = drop_lon - pick_lon
    a = np.sin(lat / 2) ** 2 + \
        np.cos(pick_lat) * np.cos(drop_lat) * np.sin(lon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return earth_radius*c
    
model = joblib.load("gradient_boosting_taxi_fare_model.joblib")
st.title("NYC Taxi Fare Predictor 🚖")
st.header("Trip Details")
pickup_lat = st.number_input("Pickup Latitude", min_value=40.0, max_value=41.0, value=40.758896)
pickup_lon = st.number_input("Pickup Longitude", min_value=-74.05, max_value=-73.75, value=-73.985130)

dropoff_lat = st.number_input("Dropoff Latitude", min_value=40.0, max_value=41.0, value=40.761581)
dropoff_lon = st.number_input("Dropoff Longitude", min_value=-74.05, max_value=-73.75, value=-73.979250)

pickup_datetime = st.date_input("Pickup Date", value=datetime.today())
pickup_time = st.time_input(
    "Pickup Time",
    value=datetime.now().replace(second=0, microsecond=0).time()  # remove seconds/microseconds
)
pickup_dt = datetime.combine(pickup_datetime, pickup_time)

passenger_count = st.number_input("Number of Passengers", min_value=1, max_value=6, value=1)
distance_km = get_distance_by_haversine(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
time_taken_min = st.number_input("Time Taken (minutes)", min_value=1.0, value=10.0)

log_distance = np.log1p(distance_km)
log_time = np.log1p(time_taken_min)
hour_of_the_day = pickup_dt.hour
day_of_week = pickup_dt.weekday()

input_df = pd.DataFrame({
    'log_time': [log_time],
    'log_distance': [log_distance],
    'hour_of_the_day': [hour_of_the_day],
    'passenger_count': [passenger_count],
    'day_of_week': [day_of_week]
})

if st.button("Predict Fare"):
    predicted_log_fare = model.predict(input_df)[0]
    predicted_fare = np.expm1(predicted_log_fare)
    st.success(f"Estimated Taxi Fare: ${predicted_fare:.2f}")
