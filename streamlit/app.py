import streamlit as st

st.set_page_config(
    page_title="NYC EV Taxi Intelligence",
    page_icon="🚕",
    layout="wide"
)

st.title("NYC EV Taxi Intelligence System")
st.markdown("""
A data-driven recommendation system for EV taxi drivers in New York City.  
Built on **3 years of NYC TLC trip data**, spatial indexing with **H3 (resolution 8)**,  
and a **Gradient Boosted Tree** demand forecasting model (R² = 0.75).

---

### Navigate using the sidebar:
-  **Demand Heatmap** — Predicted pickup demand across NYC by hour
-  **Driver Simulator** — Get real-time zone recommendations
-  **EV Charger Coverage** — Charging station locations overlaid on demand

---
""")

col1, col2, col3 = st.columns(3)
col1.metric("Trip Records", "3 Years")
col2.metric("H3 Cells (Res 8)", "~800 active")
col3.metric("Demand Model R²", "0.75")