# Checkout interview — convenience wrappers around Gradle + Docker Compose.
# Run `make help` for the target list.
GRADLE := ./gradlew
COMPOSE := docker compose

# Emitter default args (override: make emitter ARGS="--topic t --count 10")
ARGS ?= --topic checkout.events.raw --count 100

.PHONY: help build test clean stage docker-build up up-d down logs ps emitter

help:
	@echo "Targets:"
	@echo "  make build         Compile all modules (no tests)"
	@echo "  make test          Run all tests (needs Docker for Testcontainers)"
	@echo "  make clean         Gradle clean"
	@echo "  make stage         Stage app jars + runtime deps for Docker (prepareDockerContext)"
	@echo "  make docker-build  Stage + 'docker compose build' (compiles everything)"
	@echo "  make up            Stage + 'docker compose up --build' (foreground, compiles everything)"
	@echo "  make up-d          Stage + 'docker compose up --build -d' (detached)"
	@echo "  make down          'docker compose down -v'"
	@echo "  make logs          'docker compose logs -f'"
	@echo "  make ps            'docker compose ps -a'"
	@echo "  make emitter       Run the one-shot emitter (override: ARGS=\"--topic t --count 10\")"

build:
	$(GRADLE) build -x test

test:
	$(GRADLE) test

clean:
	$(GRADLE) clean

stage:
	$(GRADLE) :checkout-ingestor:prepareDockerContext :checkout-quality-check:prepareDockerContext :checkout-count:prepareDockerContext :checkout-event-emitter:prepareDockerContext

docker-build: stage
	$(COMPOSE) build

up: stage
	$(COMPOSE) up --build

up-d: stage
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps -a

emitter: stage
	$(COMPOSE) build checkout-event-emitter
	$(COMPOSE) run --rm checkout-event-emitter $(ARGS)
