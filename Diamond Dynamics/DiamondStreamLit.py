import streamlit as st
import joblib
import pandas as pd

# =========================================================
# App Configuration
# =========================================================
st.set_page_config(
    page_title="💎 Diamond Intelligence App",
    layout="centered"
)

st.title("💎 Diamond Price & Market Segment Predictor")
st.caption("ML-powered diamond pricing & market segmentation")

# =========================================================
# Load Models
# =========================================================
@st.cache_resource
def load_models():
    scaler = joblib.load("diamond_cluster_scaler.pkl")
    price_model = joblib.load("diamond_price_xgboost.pkl")
    cluster_model = joblib.load("diamond_kmeans.pkl")
    return scaler, price_model, cluster_model

scaler, price_model, cluster_model = load_models()

# =========================================================
# Encoding Maps (MATCH TRAINING)
# =========================================================
cut_map = {
    'Fair': 1,
    'Good': 2,
    'Very Good': 3,
    'Premium': 4,
    'Ideal': 5
}

color_map = {
    'J': 1,
    'I': 2,
    'H': 3,
    'G': 4,
    'F': 5,
    'E': 6,
    'D': 7
}

clarity_map = {
    'I1': 1,
    'SI2': 2,
    'SI1': 3,
    'VS2': 4,
    'VS1': 5,
    'VVS2': 6,
    'VVS1': 7,
    'IF': 8
}

carat_cat_map = {
    'Light': 1,
    'Medium': 2,
    'Heavy': 3
}

# =========================================================
# Cluster Names
# =========================================================
cluster_names = {
    0: "Clarity-Focused Budget",
    1: "Size-Driven Premium",
    2: "Color-Priority Value",
    3: "Balanced Premium Quality"
}

# =========================================================
# User Inputs
# =========================================================
st.subheader("🔍 Enter Diamond Details")

col1, col2 = st.columns(2)

with col1:
    carat = st.number_input("Carat", min_value=0.1, step=0.01)
    cut = st.selectbox("Cut", list(cut_map.keys()))
    color = st.selectbox("Color", list(color_map.keys()))
with col2:
    clarity = st.selectbox("Clarity", list(clarity_map.keys()))
    carat_category = st.selectbox("Carat Category", list(carat_cat_map.keys()))

# =========================================================
# Prepare Input Data (ORDER MUST MATCH TRAINING)
# =========================================================
price_input = pd.DataFrame([[
    clarity_map[clarity],
    color_map[color],
    cut_map[cut],
    carat_cat_map[carat_category],
    carat
]], columns=[
    'clarity_ord',
    'color_ord',
    'cut_ord',
    'carat_category_ord',
    'carat'
])

cluster_input = pd.DataFrame([[
    carat,
    cut_map[cut],
    color_map[color],
    clarity_map[clarity],
    carat_cat_map[carat_category]
]], columns=[
    'carat',
    'cut_ord',
    'color_ord',
    'clarity_ord',
    'carat_category_ord'
])

cluster_scaled = scaler.transform(cluster_input)

# =========================================================
# Action Buttons
# =========================================================
st.divider()
btn1, btn2 = st.columns(2)

with btn1:
    if st.button("💰 Predict Price"):
        price = price_model.predict(price_input)[0]
        st.success(f"💎 Estimated Diamond Price: ₹ {price:,.0f}")

with btn2:
    if st.button("📊 Predict Market Segment"):
        cluster = cluster_model.predict(cluster_scaled)[0]
        segment = cluster_names.get(cluster, "Unknown Segment")
        st.info(f"📌 Cluster {cluster}: {segment}")

# =========================================================
# Footer
# =========================================================
st.divider()
st.caption("XGBoost • Clustering • PCA-ready • Streamlit Deployment")