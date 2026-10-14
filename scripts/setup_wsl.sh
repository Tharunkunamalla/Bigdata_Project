#!/usr/bin/env bash
# ==============================================================================
# CrisisGuard AI: Idempotent WSL2 Environment Setup & Systemd Provisioning
# ==============================================================================
set -euo pipefail

echo "=================================================================="
echo "      Provisioning CrisisGuard Big Data Cluster in WSL2           "
echo "=================================================================="

# 1. Verify Java 11
if ! command -v java &> /dev/null; then
    echo "Installing OpenJDK 11..."
    sudo apt-get update && sudo apt-get install -y openjdk-11-jdk
fi
echo "[OK] Java Version: $(java -version 2>&1 | head -n 1)"

# 2. Verify /opt/kafka, /opt/hadoop, /opt/hive
for dir in /opt/kafka /opt/hadoop /opt/hive; do
    if [ ! -d "$dir" ]; then
        echo "Error: Directory $dir does not exist."
        exit 1
    fi
done
echo "[OK] Big Data Daemons present in /opt"

# 3. Provision Systemd Unit Files
echo "Configuring systemd service units..."
sudo tee /etc/systemd/system/kafka.service > /dev/null << 'EOF'
[Unit]
Description=Apache Kafka Distributed Message Broker (KRaft)
After=network.target

[Service]
Type=simple
User=sivasai
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
Environment=KAFKA_HEAP_OPTS=-Xms256m -Xmx512m
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/hadoop-hdfs.service > /dev/null << 'EOF'
[Unit]
Description=Apache Hadoop HDFS Cluster (NameNode & DataNode)
After=network.target

[Service]
Type=oneshot
User=sivasai
RemainAfterExit=yes
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
ExecStart=/opt/hadoop/sbin/start-dfs.sh
ExecStop=/opt/hadoop/sbin/stop-dfs.sh

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/hive-metastore.service > /dev/null << 'EOF'
[Unit]
Description=Apache Hive Metastore Service
After=network.target hadoop-hdfs.service

[Service]
Type=simple
User=sivasai
WorkingDirectory=/home/sivasai
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
Environment=HIVE_HOME=/opt/hive
Environment=SERVICE_OPTS=-Xms128m -Xmx384m
ExecStart=/opt/hive/bin/hive --service metastore
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafka hadoop-hdfs hive-metastore
echo "[OK] Systemd service units registered and enabled."

# 4. Install Python dependencies
echo "Verifying Python scientific and Big Data packages..."
pip install --break-system-packages -q pyspark==3.5.1 kafka-python-ng fastapi uvicorn "pulp<3.0" geopy pandas scikit-learn requests

echo ""
echo ">>> WSL2 Environment Setup Complete. Run scripts/start_all.sh to launch cluster. <<<"
