import streamlit as st
import mysql.connector
import pandas as pd
from datetime import date
import requests
from datetime import datetime
# ----------------------------------
# DB CONNECTION
# ----------------------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="princy",
        password="Pass123#",
        database="nasa"
    )

# ----------------------------------
# PRE-CONFIGURED QUERIES
# ----------------------------------
queries = {
    "Number Of Times Each Asteroid Approched The Earth": """with approach_count_data as (select neo_reference_id, count(neo_reference_id) as approach_count 
from nasa.close_approach group by neo_reference_id)
select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", acd.approach_count as "Approach Count" from nasa.asteroids ast join approach_count_data acd on acd.neo_reference_id = ast.id
order by acd.approach_count, ast.id;""",
    "Average Velocity Of Each Asteroid Over Multiple Approaches": """with average_velocity_data as (select neo_reference_id, avg(relative_velocity_kmph) as average_velocity 
from nasa.close_approach group by neo_reference_id) select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name",
avd.average_velocity as "Average Velocity (kmph)" from nasa.asteroids ast 
join average_velocity_data avd on ast.id = avd.neo_reference_id order by avd.average_velocity, ast.id;""",
    "Top 10 Fastest Asteroids Based On Average Velocity": """with average_velocity_data as (select neo_reference_id, avg(relative_velocity_kmph) as average_velocity
from nasa.close_approach group by neo_reference_id)
select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", avd.average_velocity as "Average Velocity (kmph)"
from nasa.asteroids ast join average_velocity_data avd 
on ast.id = avd.neo_reference_id order by avd.average_velocity desc limit 10;""",
    "Asteroids Whose Closest Approach Is Getting Nearer Over Time": """WITH ordered AS (
    SELECT
        neo_reference_id,
        close_approach_date,
        miss_distance_km,
        LAG(miss_distance_km) OVER (
            PARTITION BY neo_reference_id
            ORDER BY close_approach_date
        ) AS previous_distance
    FROM nasa.close_approach
),
check_decreasing AS (
    SELECT
        neo_reference_id,
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN miss_distance_km < previous_distance THEN 1 ELSE 0 END)
            THEN 1 ELSE 0
        END AS consistently_decreasing
    FROM ordered
    WHERE previous_distance IS NOT NULL
    GROUP BY neo_reference_id
)
SELECT distinct cd.neo_reference_id as "Asteroid Id", ast.name as "Asteroid Name"
FROM check_decreasing cd join nasa.asteroids ast on cd.neo_reference_id = ast.id
WHERE consistently_decreasing = 1;""", 
    "Top 10 Fastest Asteroids Based On Single Approach": """with fast_approaches as (select neo_reference_id, max(relative_velocity_kmph) as max_velocity 
from nasa.close_approach group by neo_reference_id)
select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", fa.max_velocity as "Maximum Velocity"
from nasa.asteroids ast join fast_approaches fa on ast.id=fa.neo_reference_id
order by fa.max_velocity desc limit 10;""",
    "Potentially Hazardous Asteroids That Approached Earth More Than Three Times":  """with approach_count_data as (select neo_reference_id, count(neo_reference_id) as approach_count 
from nasa.close_approach group by neo_reference_id)
select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name" ,
acd.approach_count as "Approach Count" from nasa.asteroids ast join approach_count_data acd on acd.neo_reference_id = ast.id
where acd.approach_count > 3 and ast.is_potentially_hazardous_asteroid = 1;""",
    "Month With The Most Asteroid Approches": """select monthname(close_approach_date) as "Month", count(neo_reference_id) as "Asteroid Count"
from nasa.close_approach group by monthname(close_approach_date) 
order by "Asteroid Count" desc limit 1;""",
    "Asteroid With The Fastest Approach Speed": """select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", ca.relative_velocity_kmph as "Relative Velocity"
from nasa.asteroids ast join nasa.close_approach ca on ast.id = ca.neo_reference_id 
where ca.relative_velocity_kmph = (select max(relative_velocity_kmph) from nasa.close_approach);""",
    "Asteroids Sorted By Maximum Estimated Diameter": """select distinct id as "Asteroid Id", name as "Asteroid Name", estimated_diameter_max_km as "Maximum Estimated Diameter"
from nasa.asteroids order by estimated_diameter_max_km desc;""",
    "Asteroids That Came Within 0.05 AU": """select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", ca.astronomical_au as "Astronomical Distance(AU)"
from nasa.asteroids ast join nasa.close_approach ca on ast.id = ca.neo_reference_id where ca.astronomical_au < 0.05
order by ca.astronomical_au, ast.id;""",
    "Asteroids That Passed Closer Than The Moon": """select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", ca.close_approach_date as "Close Approach Date", ca.miss_distance_lunar as "Lunar Miss Dsitance"
from nasa.asteroids ast join nasa.close_approach ca
on ast.id = ca.neo_reference_id where ca.miss_distance_lunar <1
order by ca.miss_distance_lunar, ca.close_approach_date, ast.id;""",
    "Count Of Hazardous And Non-Hazardous Asteroids": """select 
case
	when is_potentially_hazardous_asteroid is true then 'Hazardous' 
	else 'Non Hazardous'
	end as Hazardous_Status, 
count(distinct(id)) as "Asteroid Count" from nasa.asteroids group by is_potentially_hazardous_asteroid;""",
    "Asteroid With Highest Brightness": """SELECT id as "Asteroid Id", name as "Asteroid Name", absolute_magnitude_h as "Absolute Magnitude"
FROM nasa.asteroids ORDER BY absolute_magnitude_h ASC LIMIT 1;""",
    "Number Of Asteroid Approaches Per Month": """select monthname(close_approach_date) as "Month",
count(*) as "Approach Count" from nasa.close_approach group by (monthname(close_approach_date));""",
    "Asteroids That Approached Earth With Velocity > 50,000 kmh": """select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", 
ca.relative_velocity_kmph as "Relative Velocity (kmph)", ca.orbiting_body as "Orbiting Body"
from nasa.asteroids ast join nasa.close_approach ca on ca.neo_reference_id = ast.id
where ca.relative_velocity_kmph > 50000 and ca.orbiting_body = 'Earth';""",
    "Asteroid Names With Approach Date And Miss Distance(km)": """SELECT distinct
    ast.id as "Asteroid Id",
    ast.name as "Asteroid Name",
    ca.close_approach_date as "Close Approach Date",
    ca.miss_distance_km as "Miss Distance(km)"
FROM nasa.asteroids ast
JOIN nasa.close_approach ca
    ON ast.id = ca.neo_reference_id
WHERE ca.miss_distance_km = (
    SELECT MIN(miss_distance_km)
    FROM nasa.close_approach cap
    WHERE cap.neo_reference_id = ca.neo_reference_id
);""",
    "Top 10 Slowest Asteroids": """with average_velocity_data as (select neo_reference_id, avg(relative_velocity_kmph) as average_velocity
from nasa.close_approach group by neo_reference_id)
select distinct ast.id as "Asteroid Id", ast.name as "Asteroid Name", avd.average_velocity as "Average Velocity (kmph)"
from nasa.asteroids ast join average_velocity_data avd 
on ast.id = avd.neo_reference_id order by avd.average_velocity asc limit 10;""",
    "Hazardous Asteroids Count Each Month": """select monthname(ca.close_approach_date) as "Month", count(distinct(ast.id)) as "Asteroid Count"  from nasa.asteroids ast join nasa.close_approach ca on ast.id = ca.neo_reference_id 
where ast.is_potentially_hazardous_asteroid=1
group by monthname(ca.close_approach_date), month(ca.close_approach_date)  
order by month(ca.close_approach_date);""",
    "Non Hazardous Asteroids Count Each Month": """select monthname(ca.close_approach_date) as "Month", count(distinct(ast.id)) as "Asteroid Count"  from nasa.asteroids ast join nasa.close_approach ca on ast.id = ca.neo_reference_id 
where ast.is_potentially_hazardous_asteroid=0
group by monthname(ca.close_approach_date), month(ca.close_approach_date)  
order by month(ca.close_approach_date);""",
    "Largest Potentially Hazardous Asteroid": """select id as "Id", name as "Asteroid Name", estimated_diameter_max_km as "Diameter(km)"
from nasa.asteroids where is_potentially_hazardous_asteroid=1 order by estimated_diameter_max_km desc limit 1;""",
    "Largest potentially Non hazardous Asteroid": """select id as "Id", name as "Asteroid Name", estimated_diameter_max_km as "Diameter(km)"
from nasa.asteroids where is_potentially_hazardous_asteroid=0 order by estimated_diameter_max_km desc limit 1;""",
    "Largest Asteroid That Went Closest To The Moon": """select ast.id as "Asteroid Id", ast.name as "Asteroid Name", ast.estimated_diameter_max_km as "Diameter(km)",
ca.miss_distance_lunar as "Miss Distance(Lunar)" from nasa.asteroids ast join nasa.close_approach ca
on ast.id = ca.neo_reference_id order by ca.miss_distance_lunar asc, ast.estimated_diameter_max_km desc
limit 1;""",
    "Smallest Asteroid That Went Closest To The Moon": """select ast.id as "Asteroid Id", ast.name as "Asteroid Name", ast.estimated_diameter_max_km as "Diameter(km)",
ca.miss_distance_lunar as "Miss Distance(Lunar)" from nasa.asteroids ast join nasa.close_approach ca
on ast.id = ca.neo_reference_id order by ca.miss_distance_lunar asc, ast.estimated_diameter_max_km asc
limit 1;""",
    "Count Of Approaches Each Month With Relative Velocity < 5000 kmph": """select monthname(close_approach_date) as "Month",
count(*) as "Approach Count" from nasa.close_approach where relative_velocity_kmph < 5000
group by (monthname(close_approach_date));"""
}
st.set_page_config(layout="wide")
st.title("🌌 NASA Asteroid Data Explorer")

col_left, col_right = st.columns([1, 3])

with col_left:
    st.header("Mode")
    mode = st.radio("Choose Mode", ["Filter Criteria", "Pre-configured Queries"])

with col_right:

#Filter Criteria
    if mode == "Filter Criteria":
        st.header("Filter Asteroid Approaches")

        f1, f2, f3 = st.columns(3)
        with f1:
            start_date = st.date_input("Start Date", value=date(2000, 1, 1))
        with f2:
            end_date = st.date_input("End Date", value=date.today())
        with f3:
            velocity_min = st.slider("Min Velocity (km/h)", 0, 150000, 0)

        f4, f5, f6 = st.columns(3)
        with f4:
            au_limit = st.slider("Max AU", 0.0, 1.0, 1.0)
        with f5:
            ld_limit = st.slider("Max LD", 0.0, 20.0, 20.0)
        with f6:
            hazardous = st.selectbox("Hazardous?", ["All", "Hazardous", "Not Hazardous"])

        f7, f8 = st.columns(2)
        with f7:
            diameter_min = st.number_input("Min Diameter (km)", 0.0, 50.0, 0.0)
        with f8:
            diameter_max = st.number_input("Max Diameter (km)", diameter_min, 50.0, 50.0)

        conditions = [
            f"ca.close_approach_date BETWEEN '{start_date}' AND '{end_date}'",
            f"ca.astronomical_au <= {au_limit}",
            f"ca.miss_distance_lunar <= {ld_limit}",
            f"ca.relative_velocity_kmph >= {velocity_min}",
            f"ast.estimated_diameter_min_km >= {diameter_min}",
            f"ast.estimated_diameter_max_km <= {diameter_max}"
        ]

        if hazardous == "Hazardous":
            conditions.append("ast.is_potentially_hazardous_asteroid = TRUE")
        elif hazardous == "Not Hazardous":
            conditions.append("ast.is_potentially_hazardous_asteroid = FALSE")

        filter_query = f"""
            SELECT 
                ast.id, ast.name,
                ast.estimated_diameter_min_km,
                ast.estimated_diameter_max_km,
                ast.is_potentially_hazardous_asteroid,
                ca.close_approach_date,
                ca.relative_velocity_kmph,
                ca.astronomical_au,
                ca.miss_distance_lunar
            FROM asteroids ast
            JOIN close_approach ca ON ca.neo_reference_id = ast.id
            WHERE {" AND ".join(conditions)}
            ORDER BY ca.close_approach_date DESC;
        """

        conn = get_connection()
        df = pd.read_sql(filter_query, conn)
        conn.close()

        st.subheader("Filtered Results")
        st.dataframe(df, use_container_width=True)

#PRE-CONFIGURED QUERIES

    else:
        st.header("Pre-Configured Queries")

        selected = st.selectbox("Choose a query", list(queries.keys()))
        sql = queries[selected]

        conn = get_connection()
        df = pd.read_sql(sql, conn)
        conn.close()

        st.subheader("Results")
        st.dataframe(df, use_container_width=True)