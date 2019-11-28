# This Makefile builds
# Output will be in: ./out

YELLOW=\e[33m
GREEN=\e[32m
BLUE=\e[34m
DEFAULT=\e[0m

SERVER_LOG?="./out/server.log"
CLIENT_LOG?="./out/client.log"

all: clean build

.PHONY: all clean build prebuild regen
clean:
	@echo "${GREEN} = Cleaning up the build environment...${DEFAULT}"
	rm -rf ./out
	@echo "${GREEN} === DONE Cleaning === ${DEFAULT}"

regen:
	@echo "${GREEN} = Regenerating mbroker...${DEFAULT}"
	protoc -I mbroker/ mbroker/mbroker.proto --go_out=plugins=grpc:mbroker
	@echo "${GREEN} === DONE Regen === ${DEFAULT}"

prebuild:
	mkdir -p ./out
	@echo "${GREEN} === DONE === ${DEFAULT}"

build: prebuild regen
	@echo "${YELLOW} = Building ... ${DEFAULT}"
	go build -o ./out/grpc_service ./grpc_service/
	go build -o ./out/grpc_consumer ./grpc_consumer/
	go build -o ./out/grpc_broker ./grpc_broker/
	@echo "${GREEN} === DONE === ${DEFAULT}"

run_broker:
	@echo "${YELLOW} = Attempting to run broker ... ${DEFAULT}"
	./out/grpc_broker

run_service:
	@echo "${YELLOW} = Attempting to run service ... ${DEFAULT}"
	./out/grpc_service

run_consumer:
	@echo "${YELLOW} = Attempting to run client ... ${DEFAULT}"
	./out/grpc_consumer

run: run_server run_client
	@echo "${GREEN} === DONE === ${DEFAULT}"

kill:
	@echo "${YELLOW} = Killing processes... ${DEFAULT}"
	ps -ef | grep grpc | awk '{print $$2}' | xargs kill -9
	@echo "${GREEN} === DONE === ${DEFAULT}"

go_check:
	@echo "${YELLOW} = Checking go code for formatting smells...${DEFAULT}"
	gofmt -l -e -d -s ./.
	@echo "${YELLOW} = Checking go code for suspicious constructs...${DEFAULT}"
	go vet ./...
	@echo "${GREEN} === DONE Checking === ${DEFAULT}"

go_correct:
	@echo "${YELLOW} = Correcting go code for formatting smells...${DEFAULT}"
	gofmt -s -w ./.
	@echo "${GREEN} === DONE Correcting === ${DEFAULT}"
