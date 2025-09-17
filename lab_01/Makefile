up-cluster:
	cd cluster && docker compose up -d

up-client:
	cd client && docker compose up -d

down-cluster:
	@cd cluster && docker compose down

down-client:
	@cd client && docker compose down

run-cluster:
	cd cluster && python main.py 30

run-cluster-lag:
	cd cluster && python main_lag.py 30

run-client:
	docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/main.py 20

.PHONY: up-cluster up-client down-cluster down-client run-cluster run-cluster-lag run-client