import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from confluent_kafka import Consumer
from streamlit_autorefresh import st_autorefresh

# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': topic_name,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])  # Subscribe to the topic
    return consumer  # Ensure the consumer is returned correctly

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    data = []
    while True:  # Add a retry loop to poll continuously
        message = consumer.poll(timeout=1.0)  # Poll for one message
        if message is None:  # No message received
            continue
        if message.error():  # Handle Kafka errors
            st.error(f"Kafka error: {message.error()}")
            continue
        try:
            value = message.value().decode('utf-8')  # Decode the message
            data.append(json.loads(value))
        except Exception as e:
            st.error(f"Error decoding message: {e}")
        break  # Exit after one message for real-time updates
    return data

# Function to plot a bar chart for lap times
def plot_bar_chart(data):
    plt.figure(figsize=(10, 6))
    for driver_id in data['driverId'].unique():
        driver_data = data[data['driverId'] == driver_id]
        plt.bar(driver_data['lap'], driver_data['Time_Lap'], label=f'Driver: {driver_id}')
    plt.xlabel('Lap')
    plt.ylabel('Time (s)')
    plt.title('Lap Times per Driver')
    plt.legend()
    plt.tight_layout()
    return plt

# Function to plot a line chart for lap times
def plot_line_chart(data):
    plt.figure(figsize=(10, 6))
    for driver_id in data['driverId'].unique():
        driver_data = data[data['driverId'] == driver_id]
        plt.plot(driver_data['lap'], driver_data['Time_Lap'], marker='o', label=f'Driver: {driver_id}')
    plt.xlabel('Lap')
    plt.ylabel('Time (s)')
    plt.title('Lap Times per Driver')
    plt.legend()
    plt.tight_layout()
    return plt

# Sidebar layout for auto-refresh
def sidebar():
    st.sidebar.header("Settings")
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

# Main function to fetch and visualize data
def main():
    st.title('🏎️ F1 Real-Time Driver Statistics Dashboard')
    sidebar()

    # Kafka Consumer for the topic
    topic_name = 'f1_driver_stats_topic'
    consumer = create_kafka_consumer(topic_name)

    # Fetch data from Kafka
    data = fetch_data_from_kafka(consumer)
    if data:
        data_frame = pd.DataFrame(data)

        # Metrics Section
        st.markdown("---")
        st.metric("Total Messages Received", len(data_frame))
        latest_year = data_frame['year'].max()
        st.metric("Latest Year Processed", latest_year)

        # Visualizations
        st.markdown("---")
        st.header("Lap Times Overview")
        col1, col2 = st.columns(2)

        with col1:
            bar_chart = plot_bar_chart(data_frame)
            st.pyplot(bar_chart)

        with col2:
            line_chart = plot_line_chart(data_frame)
            st.pyplot(line_chart)

        # Display Table
        st.markdown("---")
        st.header("Raw Data Table")
        st.dataframe(
            data_frame.sort_values(by=["year", "lap", "driverId"], ascending=[False, True, True]),
            use_container_width=True
        )
    else:
        st.warning("No data received from Kafka.")

if __name__ == "__main__":
    main()
