.PHONY: build up down restart logs trigger test validate clean

build:                       ## Build Docker images
	docker-compose build

up: build                    ## Start all services
	docker-compose up -d

down:                        ## Stop all services (keep data)
	docker-compose down

restart: down up             ## Restart everything

logs:                        ## Tail service logs
	docker-compose logs -f

trigger:                     ## Trigger the DAG manually
	docker-compose exec airflow-webserver airflow dags trigger sales_demo_pipeline

test:                        ## Run dbt tests inside the container
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && DBT_PROFILES_DIR=/opt/dbt dbt test --target dev"

validate:                    ## Quick row-count check on mart tables
	docker-compose exec postgres psql -U airflow -d sales_demo_analytics \
		-c "SELECT 'fact_transactions' AS tbl, COUNT(*) FROM marts.fact_transactions UNION ALL SELECT 'dim_customer', COUNT(*) FROM marts.dim_customer UNION ALL SELECT 'dim_product', COUNT(*) FROM marts.dim_product;"

clean:                       ## Remove all containers, images, and volumes
	docker-compose down -v --rmi all

help:                        ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
