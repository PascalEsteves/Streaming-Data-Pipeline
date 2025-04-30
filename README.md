# 🏎️ F1 Real-Time Driver Comparison

This project simulates real-time data consumption for a Formula 1 race, focusing on a specific driver and comparing their live performance with historical data from previous seasons. It leverages modern technologies such as **APIs**, **Apache Kafka**, **Apache Spark**, and **Streamlit** for real-time processing and interactive visualization.

## 🚀 Objective

To enable real-time analysis of a driver's performance during a simulated race and compare each lap with past performances from previous years.

## ⚙️ Tech Stack

- **API**: Used to fetch historical data and simulate real-time feeds.  
- **Apache Kafka**: Handles real-time data streaming.  
- **Apache Spark (Structured Streaming)**: Processes and transforms streamed data in real time.  
- **Streamlit**: Provides an interactive dashboard for data visualization.

## 🔮 Future Improvements

- Support for multiple drivers simultaneously  
- Sector-based analysis and weather data integration  
- Integration with public F1 APIs (e.g., Ergast)


## Some key points:

 - I used UV to manage all the required packages and environments
 - Docker commands:
    - docker-compose up --build
    - Inside broker contaiener to create manually the topic:
        - kafka-topics \
        --create --topic historical_f1_topic \
        --bootstrap-server broker:29092 \
        --replication-factor 1 \
        --partitions 1
    - list all topics created 
        kafka-topics --list --bootstrap-server broker:29092
