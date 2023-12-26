DOCKERFILE_PATH=./build/Dockerfile
BINARY_NAME=synchronizer

IMAGE?=quay.io/kubescape/$(BINARY_NAME)
TAG?=test

build-client:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME) cmd/client/main.go

build-server:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME) cmd/server/main.go

docker-build:
	docker buildx build --platform linux/amd64 -t $(IMAGE):$(TAG) -f $(DOCKERFILE_PATH) .
docker-push:
	docker push $(IMAGE):$(TAG)
