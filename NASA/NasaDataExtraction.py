import mysql.connector
from datetime import date
import requests
from datetime import datetime

# CREATE DATABASE AND TABLES IF NOT EXISTS
username = "princy"
password = "Pass123#"
host = "localhost"
database_name = "nasa"
conn_root = mysql.connector.connect(
    host=host,
    user=username,
    password=password
)

cursor_root = conn_root.cursor()
cursor_root.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
conn_root.commit()

cursor_root.close()
conn_root.close()
conn = mysql.connector.connect(
    host=host,
    user=username,
    password=password,
    database=database_name
)

cursor = conn.cursor()

create_asteroids_table_query = """CREATE TABLE IF NOT EXISTS asteroids (
    id INT,
    name VARCHAR(50),
    absolute_magnitude_h FLOAT,
    estimated_diameter_min_km FLOAT,
    estimated_diameter_max_km FLOAT,
    is_potentially_hazardous_asteroid BOOLEAN
);"""

cursor.execute(create_asteroids_table_query)
cursor.execute("TRUNCATE TABLE nasa.asteroids;")
conn.commit()

create_close_approach_query = """
CREATE TABLE IF NOT EXISTS close_approach (
    neo_reference_id INT,
    close_approach_date DATE,
    relative_velocity_kmph FLOAT,
    astronomical_au FLOAT,
    miss_distance_km FLOAT,
    miss_distance_lunar FLOAT,
    orbiting_body VARCHAR(50)
);
"""
cursor.execute(create_close_approach_query)
cursor.execute("TRUNCATE TABLE nasa.close_approach;")
conn.commit()

# ----------------------------------
# EXTRACT DATA FROM API AND INSERT IN TABLE
# ----------------------------------


request_url = "https://api.nasa.gov/neo/rest/v1/feed?start_date=2024-01-01&end_date=2024-01-07&api_key=2Ds3ZbdC8rAkzD00Dw7wpeU8ZiEKrWndydCc25Jz"
asteroid_data = []
close_approach_data = []
while True:
    response = requests.get(request_url)
    nasa_raw_data = response.json()
    near_earth_objects = nasa_raw_data['near_earth_objects']
    for date in near_earth_objects:
        print(date)
        for asteroid in near_earth_objects[date]:
            try:
                asteroid_row = {
                    "id" : int(asteroid["id"]),
                    "name" : (asteroid["name"]),
                    "absolute_magnitude_h" : float(asteroid["absolute_magnitude_h"]),
                    "estimated_diameter_min_km" : float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"]),
                    "estimated_diameter_max_km" : float(asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"]),
                    "is_potentially_hazardous_asteroid" : bool(asteroid["is_potentially_hazardous_asteroid"])
                }
                for close_approach in asteroid["close_approach_data"]:
                    close_approach_data_row = {
                        "neo_reference_id" : int(asteroid["neo_reference_id"]),
                        "close_approach_date" : datetime.strptime(close_approach["close_approach_date"], "%Y-%m-%d").date(),
                        "relative_velocity_kmph" : float(close_approach["relative_velocity"]["kilometers_per_hour"]),
                        "astronomical" : float(close_approach["miss_distance"]["astronomical"]),
                        "miss_distance_km" : float(close_approach["miss_distance"]["kilometers"]),
                        "miss_distance_lunar" : float(close_approach["miss_distance"]["lunar"]),
                        "orbiting_body" : (close_approach["orbiting_body"])
                    }
                    close_approach_data.append(close_approach_data_row)
                asteroid_data.append(asteroid_row)
            except (KeyError, TypeError, ValueError):
                continue
    if len(asteroid_data) >= 10000 and len(close_approach_data) >= 10000:
        break        
    request_url = nasa_raw_data['links']['next']
print(len(asteroid_data))
print(len(close_approach_data))

asteroid_insert_query = """
INSERT INTO asteroids (
    id,
    name,
    absolute_magnitude_h,
    estimated_diameter_min_km,
    estimated_diameter_max_km,
    is_potentially_hazardous_asteroid
) VALUES (%s, %s, %s, %s, %s, %s)
"""

asteroid_values = [
    (
        int(row.get("id", 0)),
        row.get("name", "Unknown"),
        float(row.get("absolute_magnitude_h", 0.0)),
        float(row.get("estimated_diameter_min_km", 0.0)),
        float(row.get("estimated_diameter_max_km", 0.0)),
        bool(row.get("is_potentially_hazardous_asteroid", False)) 
    )
    for row in asteroid_data
]

cursor.executemany(asteroid_insert_query, asteroid_values[:10000])
conn.commit()
print("Inserted", cursor.rowcount, "asteroid rows.")

close_approach_insert_query = """
INSERT INTO close_approach (
    neo_reference_id,
    close_approach_date,
    relative_velocity_kmph,
    astronomical_au,
    miss_distance_km,
    miss_distance_lunar,
    orbiting_body
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

close_approach_values = [
    (
        int(row.get("neo_reference_id", 0)),
        row["close_approach_date"],
        float(row.get("relative_velocity_kmph", 0.0)),
        float(row.get("astronomical", 0.0)),               
        float(row.get("miss_distance_km", 0.0)),                
        float(row.get("miss_distance_lunar", 0.0)),            
        row.get("orbiting_body", "Unknown")                    
    )
    for row in close_approach_data
]

cursor.executemany(close_approach_insert_query, close_approach_values[:10000])
conn.commit()
print("Inserted", cursor.rowcount, "close-approach rows.")