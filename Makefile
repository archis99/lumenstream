PROJECT = lumenstream
JAR = build/libs/$(PROJECT)-1.0-SNAPSHOT.jar

.PHONY: help build clean test lint run docker-build

help:
	@echo "Usage:"
	@echo "  make build        			- build fat jar"
	@echo "  make clean        			- clean project"
	@echo "  make test         			- run tests"
	@echo "  make lint         			- run formatting & lint checks"

build:
	./gradlew clean shadowJar

clean:
	./gradlew clean

test:
	./gradlew test

lint:
	./gradlew spotlessApply
	./gradlew spotlessCheck