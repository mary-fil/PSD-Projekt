import json
import time
import pandas as pd
import streamlit as st
import altair as alt
from kafka import KafkaConsumer

st.set_page_config(page_title="Panel Monitorowania Oszustw", layout="wide")

class FraudDashboardConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='alerts'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        if 'alerts_history' not in st.session_state:
            st.session_state.alerts_history = []

    def fetch_new_messages(self):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='streamlit_soc_dashboard_v2',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=500
            )
            
            new_alerts = []
            for msg in consumer:
                new_alerts.append(msg.value)
            consumer.close()

            if new_alerts:
                new_alerts.reverse() 
                st.session_state.alerts_history = new_alerts + st.session_state.alerts_history
                st.session_state.alerts_history = st.session_state.alerts_history[:1000]
                
        except Exception:
            pass

    def render_kpi_section(self, df):
        if df.empty:
            st.info("Oczekiwanie na dane...")
            return

        total_alerts = len(df)
        max_amount = df['amount'].max() if 'amount' in df.columns else 0.0
        unique_cards = df['card_id'].nunique() if 'card_id' in df.columns else 0
        
        counts = df['alert_type'].value_counts()
        amount_count = counts.get('AMOUNT_ANOMALY', 0)
        location_count = counts.get('LOCATION_ANOMALY', 0)
        freq_count = counts.get('FREQUENCY_ANOMALY', 0)

        c1, c2, c3 = st.columns(3)
        c1.metric("Liczba alertów", total_alerts)
        c2.metric("Najwyższa kwota (PLN)", f"{max_amount:,.2f}")
        c3.metric("Zagrożone karty", unique_cards)

        c4, c5, c6 = st.columns(3)
        c4.metric("Anomalie kwotowe", amount_count)
        c5.metric("Anomalie lokalizacyjne", location_count)
        c6.metric("Anomalie częstotliwości", freq_count)

        st.divider()

    def render_charts_section(self, df):
        if df.empty:
            return

        c1, c2, c3 = st.columns(3)

        with c1:
            st.subheader("Anomalie Kwotowe")
            amount_df = df[df['alert_type'] == 'AMOUNT_ANOMALY'].copy()
            if not amount_df.empty:
                chart = alt.Chart(amount_df).mark_bar().encode(
                    x=alt.X('card_id', title='ID Karty'),
                    y=alt.Y('amount', title='Kwota (PLN)')
                )
                st.altair_chart(chart, use_container_width=True)

        with c2:
            st.subheader("Anomalie Częstotliwości")
            freq_df = df[df['alert_type'] == 'FREQUENCY_ANOMALY'].copy()
            if not freq_df.empty:
                freq_counts = freq_df['card_id'].value_counts().reset_index()
                freq_counts.columns = ['card_id', 'count']
                chart = alt.Chart(freq_counts).mark_bar().encode(
                    x=alt.X('card_id', title='ID Karty'),
                    y=alt.Y('count', title='Liczba użyć')
                )
                st.altair_chart(chart, use_container_width=True)

        with c3:
            st.subheader("Anomalie Lokalizacyjne")
            loc_df = df[df['alert_type'] == 'LOCATION_ANOMALY'].copy()
            if not loc_df.empty:
                loc_df['distance_km'] = loc_df['details'].str.extract(r'na dystansie ([\d\.]+) km').astype(float)
                loc_max = loc_df.groupby('card_id')['distance_km'].max().reset_index()
                chart = alt.Chart(loc_max).mark_bar().encode(
                    x=alt.X('card_id', title='ID Karty'),
                    y=alt.Y('distance_km', title='Dystans (km)')
                )
                st.altair_chart(chart, use_container_width=True)

    def render_table_section(self, df):
        st.subheader("Strumień Alertów")
        if df.empty:
            return

        for _, row in df.head(50).iterrows():
            label = f"{row.get('timestamp', 'N/A')} | {row.get('alert_type', 'N/A')} | {row.get('card_id', 'N/A')}"
            with st.expander(label):
                st.json(row.to_dict())

    def run(self):
        st.title("Panel Monitorowania Oszustw")
        self.fetch_new_messages()
        df = pd.DataFrame(st.session_state.alerts_history)
        
        self.render_kpi_section(df)
        self.render_charts_section(df)
        st.divider()
        self.render_table_section(df)

        time.sleep(2)
        st.rerun()

if __name__ == "__main__":
    app = FraudDashboardConsumer()
    app.run()