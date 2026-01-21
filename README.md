# Couturae - Fashion Week Data Platform

Real-time fashion trend analysis platform tracking buzzwords and designers from fashion week coverage.

## Quick Start

### Prerequisites
- Docker Desktop running
- Node.js installed

### Setup
```bash
npm install
```

### Run Everything
```bash
make start
```

This starts:
- PostgreSQL database
- Kafka message broker
- API server on http://localhost:3000
- Normalizer service
- Dashboard on http://localhost:8080

### Fetch Data
```bash
make ingest
```

### Stop Everything
```bash
make stop
```

### Clean Reset
```bash
make clean
```

## Manual Commands

Start services:
```bash
npm start
```

Run ingestor:
```bash
npm run ingest
```

Stop services:
```bash
npm stop
```

## Architecture

- **Ingestor**: Fetches RSS feeds from fashion news sites
- **Normalizer**: Extracts fashion keywords and designer mentions
- **API**: Provides REST endpoints for data access
- **Dashboard**: Web interface displaying trends

## Endpoints

- Dashboard: http://localhost:8080/dashboard.html
- API Stats: http://localhost:3000/api/stats
- Trending Buzzwords: http://localhost:3000/api/buzzwords/trending
