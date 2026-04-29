# Deel Analytics Platform — Solution

Complete analytics platform solution, ready to run locally.

## Quick Start

```bash
# 1. Copy .env.example to .env and populate credentials
cp .env.example .env

# 2. Start all services
docker-compose up -d

# 3. Wait ~90s for all services to become healthy
docker-compose ps
