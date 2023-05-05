.PHONY: start.kafka

start.kafka:
	docker compose up -d kraft

run.consumer:
	docker exec -it kraft kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(TOPIC)

run.producer:
	docker exec -it kraft kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $(TOPIC)
