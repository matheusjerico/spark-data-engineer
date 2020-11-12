.PHONY: setup run

setup:
	@echo "Construindo a Image da aplicação."
	docker image build . -f environment/Dockerfile -t spark/spark-processing:latest

run:
	@echo "Executando o Container."
	docker container run -v $(shell pwd)/container-result:/app/result -p 8989:8989 spark/spark-processing:latest