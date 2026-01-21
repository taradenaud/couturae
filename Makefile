.PHONY: start stop ingest clean logs

start:
	@echo "Starting Docker services..."
	@docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Starting application services..."
	@npm run dev

stop:
	@echo "Stopping application services..."
	@pkill -f "ts-node" || true
	@pkill -f "http-server" || true
	@echo "Stopping Docker services..."
	@docker-compose down

ingest:
	@cd services/ingestor && npx ts-node index.ts

clean:
	@echo "Stopping all services..."
	@make stop
	@echo "Cleaning database..."
	@docker-compose down -v
	@echo "Reinstalling dependencies..."
	@npm install

logs:
	@docker-compose logs -f
