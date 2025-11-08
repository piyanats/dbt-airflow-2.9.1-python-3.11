.PHONY: help init build up down restart logs clean test deploy-gke setup-wi

# Default target
help:
	@echo "Available commands:"
	@echo "  make init          - Initialize the project (first time setup)"
	@echo "  make build         - Build Docker images"
	@echo "  make up            - Start Airflow services"
	@echo "  make down          - Stop Airflow services"
	@echo "  make restart       - Restart Airflow services"
	@echo "  make logs          - View Airflow logs"
	@echo "  make clean         - Clean up Docker resources"
	@echo "  make test-dbt      - Test dbt project locally"
	@echo "  make deploy-gke    - Deploy to GKE"
	@echo "  make setup-wi      - Setup GKE Workload Identity"

# Initialize project
init:
	@echo "Initializing project..."
	@cp .env.example .env
	@echo "Please edit .env with your configuration"
	@echo "Then run: make build && make up"

# Build Docker images
build:
	docker-compose build

# Start Airflow services
up:
	@echo "Initializing Airflow database..."
	docker-compose up airflow-init
	@echo "Starting Airflow services..."
	docker-compose up -d
	@echo "Airflow is starting. Access UI at http://localhost:8080"
	@echo "Username: airflow, Password: airflow"

# Stop Airflow services
down:
	docker-compose down

# Restart Airflow services
restart:
	docker-compose restart

# View logs
logs:
	docker-compose logs -f

# Clean up Docker resources
clean:
	docker-compose down -v
	docker system prune -f

# Test dbt project
test-dbt:
	@echo "Testing dbt project..."
	cd dbt_project && dbt debug && dbt run --target dev && dbt test --target dev

# Deploy to GKE
deploy-gke:
	@echo "Deploying to GKE..."
	@echo "Make sure you have:"
	@echo "  1. Built and pushed the Docker image"
	@echo "  2. Updated config/airflow-gke-deployment.yaml with your project ID"
	@echo "  3. Generated and set the Fernet key in the Secret"
	kubectl apply -f config/airflow-gke-deployment.yaml
	@echo "Deployment submitted. Check status with: kubectl get pods -n airflow"

# Setup GKE Workload Identity
setup-wi:
	@echo "Setting up GKE Workload Identity..."
	./config/setup-workload-identity.sh

# Build and push Docker image to GCR
push-image:
	@echo "Building and pushing Docker image..."
	@test -n "$(GCP_PROJECT_ID)" || (echo "GCP_PROJECT_ID is not set" && exit 1)
	docker build -t gcr.io/$(GCP_PROJECT_ID)/airflow-dbt:latest .
	docker push gcr.io/$(GCP_PROJECT_ID)/airflow-dbt:latest
	@echo "Image pushed to gcr.io/$(GCP_PROJECT_ID)/airflow-dbt:latest"

# Setup Airflow configuration
setup-airflow:
	@echo "Setting up Airflow connections and variables..."
	docker-compose exec airflow-webserver bash /opt/airflow/config/setup-airflow-config.sh
