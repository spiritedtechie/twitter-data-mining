# Download and install kafka
KAFKA_VERSION="kafka_2.11-2.0.0"

wget -nc http://apache.mirror.anlx.net/kafka/2.0.0/${KAFKA_VERSION}.tgz

if [ ! -d "./${KAFKA_VERSION}" ]
then
    tar -xvf ${KAFKA_VERSION}.tgz
    ln -s ${KAFKA_VERSION} kafka
fi

# Start kafka cluster and zookeeper node
./kafka_2.11-2.0.0/bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
./kafka_2.11-2.0.0/bin/kafka-server-start.sh -daemon config/server-1.properties
./kafka_2.11-2.0.0/bin/kafka-server-start.sh -daemon config/server-2.properties 
