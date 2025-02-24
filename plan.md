::: mermaid
flowchart LR
    subgraph Geração de Dados
        G[Data Generator] --> |Multiprocessing| T1[Transaction Logs]
        T1 --> |Python| KP[Kafka Producer]
    end

    subgraph Message Broker
        KP --> |JSON| K1[Kafka Topic:\nRaw Transactions]
        K1 --> |Streaming| K2[Kafka Topic:\nProcessed Data]
        K2 --> |Alerts| K3[Kafka Topic:\nFraud Alerts]
    end

    subgraph Processing
        K1 --> SP[Spark Streaming]
        SP --> |ML Pipeline| FR[Fraud Detection]
        FR --> |Classification| K2
        FR --> |Alerts| K3
    end

    subgraph Storage & Analytics
        K2 --> IF[(InfluxDB)]
        K3 --> IF
    end

    subgraph Visualization
        IF --> |Real-time| GR[Grafana Dashboard]
    end
:::