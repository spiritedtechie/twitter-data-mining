# Download and install kafka
KAFKA_VERSION="kafka_2.11-2.0.0"

wget -nc http://apache.mirror.anlx.net/kafka/2.0.0/${KAFKA_VERSION}.tgz

if [ ! -d "./${KAFKA_VERSION}" ]
then
    tar -xvf ${KAFKA_VERSION}.tgz
    ln -s ${KAFKA_VERSION} kafka
fi

# Make data directories
sudo mkdir /var/lib/zookeeper
sudo mkdir /var/lib/kafka
sudo chown $USER /var/lib/zookeeper
sudo chown $USER /var/lib/kafka

# Start kafka cluster and zookeeper node
./kafka/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
./kafka/bin/kafka-server-start.sh -daemon config/server-1.properties
./kafka/bin/kafka-server-start.sh -daemon config/server-2.properties 

# Create topics - 2 partitions and 1 replica for the simple setup of two local nodes
./kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic twitter-data

# Describe topic setup
./kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
./kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic twitter-data

