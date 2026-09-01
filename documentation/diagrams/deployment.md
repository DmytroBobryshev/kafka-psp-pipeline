# Deployment (kind)

```mermaid
flowchart TB
    subgraph host [Your machine - Docker Desktop]
        subgraph kind [kind cluster kafka-psp - 3 nodes]
            subgraph nsK [namespace kafka]
                STR[Strimzi operator] --> KAFKA[Kafka 4.3 KRaft\n3 combined nodes\nSASL/SCRAM + ACLs]
                KC[KafkaConnect\nDebezium outbox + Mongo audit sink]
                APPS[payment-api psp-connector ledger\nwebhook-notifier analytics realtime-gateway\napi-gateway discovery-server ui]
                PG[(PostgreSQL)]
                MG[(MongoDB)]
                MINIO[(MinIO - claim check)]
                SR[Schema Registry]
                KEDA[KEDA - lag autoscaling]
            end
            subgraph nsM [namespace monitoring]
                PROM[Prometheus] --> GRAF[Grafana]
                EXP[kafka-exporter - consumer lag]
            end
            ING[ingress-nginx - hostPort 80]
        end
    end
    BROWSER[Browser http://localhost] --> ING --> APPS
    APPS --> KAFKA & PG & MG & SR
    KEDA -. scales on lag .-> APPS
    PROM --> EXP & KAFKA
```

Brought up by three scripts: `up.sh` (Kafka platform) → `deploy-apps.sh` (build + deploy apps)
→ `install-monitoring.sh` (Prometheus/Grafana). See the root README for the from-zero guide.
