# Compose up
    docker-compose up

# Create topic
    docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \ --create \ --bootstrap-server localhost:9092 \ --replication-factor 1 \ --partitions 1 \ --topic user

# Install libs
    npm install

# Run microservices
    npm run start:gateway
    npm run start:email

# List messages from one topic
    docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \ --bootstrap-server localhost:9092 \ --from-beginning \ --topic user