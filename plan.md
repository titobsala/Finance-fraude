flowchart TB
    subgraph Data_Generation
        A[Log Generator] --> |Simulated Payment Data| B[Kafka Producer]
    end
    
    subgraph Kafka_Hub
        B --> C[Kafka Broker]
        C --> |Transaction Topic| D[Spark Streaming Consumer]
        C --> |Raw Logs Topic| F[Storage Consumer]
    end
    
    subgraph Processing
        D --> E[Spark Streaming]
        E --> |Real-time Processing| G[Fraud Detection Model]
        E --> |Aggregated Data| H[Time Series DB]
    end
    
    subgraph Storage
        F --> I[Data Lake]
        I --> J[Batch Processing]
        J --> |Model Training| G
    end
    
    subgraph Visualization
        H --> K[Grafana Dashboard]
        G --> |Fraud Alerts| K
    end