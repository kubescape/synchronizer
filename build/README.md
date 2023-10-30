## Docker Build

### Build your own Docker image

Run the following command to build your own Docker image:
```
docker buildx build -t synchronizer -f build/Dockerfile --load .
```