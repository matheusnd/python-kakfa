install_libs:
	@pip3 install -r requirements.txt

run_producer:
	@python3 python-producer/producer.py

run_consumer:
	@python3 python-consumer/consumer.py

start:
	@docker-compose up -d

stop:
	@docker-compose stop

run_twitter_producer:
	@python3 twitter-producer/producer.py

create_topic:
	@docker-compose exec kafka kafka-topics --zookeeper localhost:32181 --topic python_twiter --create --partitions 3 --replication-factor 1
