.PHONY: up down logs build

# Rebuild when Dockerfile or copied sources change (Docker layer cache); then start detached.
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

build:
	docker compose build
