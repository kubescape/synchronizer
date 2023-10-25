# AsyncAPI specification

To update the specification, edit the `asyncapi.yaml` file and run:

```bash
asyncapi generate models golang api/asyncapi.yaml --packageName=domain -o domain && gofmt -w .
```
