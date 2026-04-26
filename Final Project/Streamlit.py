# =========================
# IMPORTS (make sure these are at top of file)
# =========================
import streamlit as st
import torch
import torch.nn as nn
import joblib
import pandas as pd
import numpy as np
import sqlite3
seq_df = pd.read_csv("seq_df_for_behavior.csv")

seq_df["order_purchase_timestamp"] = pd.to_datetime(
    seq_df["order_purchase_timestamp"]
)

from google import genai

client = genai.Client(
    api_key="AIzaSyCzpNQu98WbkC76lP8Uakmok-RFXZ1unAU"
)
conn = sqlite3.connect("E:/AI ML Projects/AI ML Course/Mini Projects/6. Final Project/olist_project.db")
# =========================
# 🔹 FEEDBACK SUMMARIZER
# =========================
def summarize_feedback(review_texts):
    reviews_combined = "\n".join(review_texts[:50])

    prompt = f"""
    You are a customer feedback analyst for an e-commerce platform.

    Analyze these customer reviews and give:
    1. Overall sentiment (Positive / Negative / Mixed)
    2. Key themes
    3. Main complaints
    4. Positive highlights
    5. Business recommendations

    Reviews:
    {reviews_combined}
    """

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return response.text


# =========================
# 🔹 DATA CHATBOT
# =========================
def ask_db(question):
    schema = """
    Table: Dim_Customers
    Columns:
    customer_id, customer_unique_id, customer_city, customer_state

    Table: Fact_Orders
    Columns:
    order_id, customer_id, order_status, order_purchase_timestamp,
    payment_value, payment_installments, item_count,
    total_price, total_freight, review_score
    """

    prompt = f"""
    You are a SQLite expert.

    Convert the question into SQL using ONLY this schema:

    {schema}

    Rules:
    - Return ONLY SQL
    - No explanation
    - No markdown
    - Use SQLite syntax
    - Join:
      Fact_Orders.customer_id = Dim_Customers.customer_id

    Question:
    {question}
    """

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()

    try:
        df = pd.read_sql(sql_query, conn)
        return sql_query, df
    except Exception as e:
        return sql_query, f"SQL Error: {e}"


# =========================
# MODEL CLASS
# =========================
class EnrichedMLP(nn.Module):
    def __init__(self, num_categories, sequence_length=2, embed_dim=16, hidden_dim=64):
        super().__init__()
        self.embedding = nn.Embedding(num_categories, embed_dim)
        input_dim = (embed_dim + 3) * sequence_length

        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.dropout = nn.Dropout(0.3)
        self.output = nn.Linear(hidden_dim, num_categories)
        self.relu = nn.ReLU()

    def forward(self, cat, num):
        emb = self.embedding(cat)
        x = torch.cat([emb, num], dim=2)
        x = x.view(x.size(0), -1)

        x = self.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.relu(self.fc2(x))
        x = self.dropout(x)

        return self.output(x)


# =========================
# LOAD MODELS
# =========================
ltv_model = joblib.load("random_forest_model.pkl")
churn_model = joblib.load("knn_model.pkl")
scaler = joblib.load("scaler.pkl")

category_mapping = joblib.load("category_mapping.pkl")
reverse_mapping = {v: k for k, v in category_mapping.items()}

# IMPORTANT FIX (your model was trained with 75)
num_categories = 74

behavior_model = EnrichedMLP(
    num_categories=num_categories,
    sequence_length=2
)
behavior_model.load_state_dict(torch.load("behavioral_mlp.pth", map_location="cpu"))
behavior_model.eval()


# =========================
# PREDICTION FUNCTION
# =========================
def predict_next_category(cat_seq, num_seq):
    cat_tensor = torch.tensor([cat_seq], dtype=torch.long)
    num_tensor = torch.tensor([num_seq], dtype=torch.float32)

    with torch.no_grad():
        output = behavior_model(cat_tensor, num_tensor)
        pred = torch.argmax(output, dim=1).item()

    return reverse_mapping.get(pred, f"Unknown ({pred})")


# =========================
# UI STARTS HERE
# =========================
st.title("Unified E-Commerce AI Dashboard")

# ---- Tabs ----
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "LTV Prediction",
    "Churn Prediction",
    "Next Category",
    "Feedback Summarizer",
    "Chatbot"
])



# =========================
# TAB 1: LTV
# =========================
with tab1:
    st.subheader("LTV Prediction")

    avg_purchase_value = st.number_input("Avg Purchase Value", 0.0, value=100.0)
    avg_freight_value = st.number_input("Avg Freight Value", 0.0, value=20.0)
    customer_lifespan = st.number_input("Customer Lifespan", 0, value=30)
    recency = st.number_input("Recency", 0, value=15)

    if st.button("Predict LTV"):
        X = pd.DataFrame([{
            "avg_purchase_value": avg_purchase_value,
            "avg_freight_value": avg_freight_value,
            "customer_lifespan": customer_lifespan,
            "recency": recency
        }])

        pred = ltv_model.predict(X)[0]
        st.success(f"Predicted LTV: {pred:.2f}")


# =========================
# TAB 2: CHURN
# =========================
with tab2:
    st.subheader("Churn Prediction")

    num_orders = st.number_input("Number of Orders", 0, value=3)
    avg_freight = st.number_input("Avg Freight", 0.0, value=25.0)
    positive_review_ratio = st.number_input("Positive Review Ratio", 0.0, 1.0, value=0.8)
    customer_lifespan = st.number_input("Customer Lifespan ", 0, value=60)
    purchase_frequency = st.number_input("Purchase Frequency", 0.0, value=0.1)

    if st.button("Predict Churn"):
        X = pd.DataFrame([{
            "num_orders": num_orders,
            "avg_freight_value": avg_freight,
            "positive_review_ratio": positive_review_ratio,
            "customer_lifespan": customer_lifespan,
            "purchase_frequency": purchase_frequency
        }])

        X_scaled = scaler.transform(X)
        pred = churn_model.predict(X_scaled)[0]

        st.success("Will Churn" if pred == 1 else "Retained")


# =========================
# TAB 3: NEXT CATEGORY
# =========================
with tab3:
    st.subheader("Next Category Prediction by Customer")

    # 🔹 Filter only customers with >= 2 orders
    customer_counts = seq_df["customer_unique_id"].value_counts()
    valid_customers = customer_counts[customer_counts >= 2].index.tolist()

    customer_list = sorted(valid_customers)

    selected_customer = st.selectbox(
        "Select Customer (≥ 2 orders only)",
        customer_list
    )

    # 🔹 Get selected customer's data
    customer_orders = seq_df[
        seq_df["customer_unique_id"] == selected_customer
    ].sort_values("order_purchase_timestamp")

    # 🔹 Show their history (this is GOOD UX)
    st.subheader("Customer Purchase History")
    st.dataframe(customer_orders[[
        "order_purchase_timestamp",
        "product_category_name",
        "review_score",
        "price",
        "freight_value"
    ]])

    # 🔹 Use last 2 purchase (since sequence_length = 2)
    last_2 = customer_orders.tail(2)

    cat_seq = last_2["category_encoded"].tolist()

    num_seq = last_2[[
        "review_score",
        "price",
        "freight_value"
    ]].values.tolist()

    if st.button("Predict Next Category"):
        pred = predict_next_category(cat_seq, num_seq)
        st.success(f"Predicted Next Category: {pred}")


with tab4:
    st.subheader("Feedback Summarizer by Rating")

    ratings_df = pd.read_sql("""
        SELECT DISTINCT review_score
        FROM Fact_Orders
        WHERE review_score IS NOT NULL
        ORDER BY review_score
    """, conn)

    ratings_list = ratings_df["review_score"].astype(int).tolist()

    selected_rating = st.selectbox(
        "Select Review Rating",
        ratings_list
    )

    df = pd.read_sql(f"""
        SELECT 
            order_id,
            review_score,
            payment_value,
            total_price,
            total_freight,
            order_status
        FROM Fact_Orders
        WHERE review_score = {selected_rating}
        LIMIT 100
    """, conn)

    st.write(f"Orders with Review Rating {selected_rating}")
    st.dataframe(df)

    if st.button("Summarize Selected Rating Reviews"):
        if df.empty:
            st.warning("No reviews found for this rating.")
        else:
            review_texts = [
                f"Customer gave rating {int(row.review_score)}. "
                f"Order status: {row.order_status}. "
                f"Payment value: {row.payment_value}. "
                f"Product price: {row.total_price}. "
                f"Freight value: {row.total_freight}."
                for _, row in df.iterrows()
            ]

            summary = summarize_feedback(review_texts)

            st.subheader(f"Summary for Rating {selected_rating}")
            st.write(summary)
# =========================
# 🔹 TAB 5: CHATBOT
# =========================
with tab5:
    st.subheader("Interactive Data Chatbot")

    question = st.text_input(
        "Ask a business question",
        placeholder="Example: Which state has highest total sales?"
    )

    if st.button("Ask"):
        if question.strip():
            sql_query, result = ask_db(question)

            st.write("Generated SQL")
            st.code(sql_query, language="sql")

            if isinstance(result, pd.DataFrame):
                st.dataframe(result)
            else:
                st.error(result)
        else:
            st.warning("Please enter a question.")