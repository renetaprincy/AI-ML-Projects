import streamlit as st
import numpy as np
from PIL import Image
import tensorflow as tf

st.set_page_config(
    page_title="Brain Tumor Detection",
    page_icon="🧠",
    layout="centered"
)

@st.cache_resource
def load_model():
    return tf.keras.models.load_model("best_inception.h5")

model = load_model()


class_names = ["Glioma", "Meningioma", "No Tumor", "Pituitary"]

st.title("🧠 Brain Tumor Detection System")
st.markdown("Upload an MRI image and get AI-based tumor prediction with confidence score.")

st.divider()
uploaded_file = st.file_uploader(
    "📤 Upload MRI Image",
    type=["jpg", "jpeg", "png"]
)
def preprocess_image(image):
    image = image.resize((224, 224))  # match model input size
    image = np.array(image)

    if image.shape[-1] == 4:  # remove alpha channel if present
        image = image[:, :, :3]

    image = image / 255.0
    image = np.expand_dims(image, axis=0)
    return image
if uploaded_file is not None:

    image = Image.open(uploaded_file)

    col1, col2 = st.columns(2)

    with col1:
        st.image(image, caption="Uploaded MRI Scan", use_container_width=True)

    processed = preprocess_image(image)

    prediction = model.predict(processed)[0]

    predicted_index = np.argmax(prediction)
    predicted_label = class_names[predicted_index]
    confidence = float(np.max(prediction))

    with col2:
        st.subheader("🧾 Prediction Result")

        st.success(f"**Tumor Type:** {predicted_label}")

        st.metric(label="Confidence Score", value=f"{confidence*100:.2f}%")

        st.write("### 📊 Class Probabilities")

        for i, label in enumerate(class_names):
            st.write(f"{label}: {prediction[i]*100:.2f}%")
            st.progress(float(prediction[i]))