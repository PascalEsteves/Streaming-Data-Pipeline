import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer


def fetch_data_from_kafka(consumer):

    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data


def plot_bar_chart(data):
    plt.figure(figsize=(12, 6))

    years = sorted(data['year'].unique())
    laps = sorted(data['lap'].unique())
    bar_width = 0.8 / len(years) 
    x = np.arange(len(laps))  

    for i, year in enumerate(years):
        group = data[data['year'] == year]
        offsets = x + i * bar_width
        positions = [group[group['lap'] == lap]['position'].values[0] if lap in group['lap'].values else np.nan for lap in laps]
        plt.bar(offsets, positions, width=bar_width, label=str(year))

    plt.xlabel('Lap')
    plt.ylabel('Position')
    plt.title('Position per Lap per Year')
    plt.xticks(x + bar_width * (len(years) - 1) / 2, laps)  
    plt.legend(title='Year')
    plt.tight_layout()
    #plt.gca().invert_yaxis() 
    return plt

def plot_line_chart(data):
    plt.figure(figsize=(10, 6))
    
    for year, group in data.groupby(["year"]):
        group = group.sort_values(by=['lap']) 
        
        plt.plot(group['lap'], group['Time_Lap'], marker='o', label=f'{year}')
    
    plt.xlabel('Sequential Laps (Across Years)')
    plt.ylabel('Time (s)')
    plt.title('Sequential Lap Times per Year')
    plt.legend()
    plt.tight_layout()
    return plt

def update_data(data):

    data_frame = pd.DataFrame(data)

    st.markdown("---")
    st.metric("Total Messages Received", len(data_frame))
    latest_year = data_frame['year'].max()
    st.metric("Latest Year Processed", latest_year)

    st.markdown("---")
    st.header("Lap Times Overview")

    bar_chart = plot_bar_chart(data_frame)
    st.pyplot(bar_chart)

    line_chart = plot_line_chart(data_frame)
    st.pyplot(line_chart)

def sidebar():

    data = []
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    topic_name = 'f1_driver_stats_topic'
    consumer = create_kafka_consumer(topic_name)
    
    data = fetch_data_from_kafka(consumer)
    print(data)
    update_data(data)
    
    if st.sidebar.button('Refresh Data'):
        data = fetch_data_from_kafka(consumer)
        update_data(data)

def main():
    st.title('🏎️ F1 Real-Time Driver Statistics Dashboard')
    sidebar()

if __name__ == "__main__":
    main()
