# AsyncAPI specification

## Pre-requisites

* Install AsyncAPI CLI: https://www.asyncapi.com/docs/tutorials/generate-code

  * MacOS:

        brew install asyncapi


  * Linux:

        curl -OL https://github.com/asyncapi/cli/releases/latest/download/asyncapi.deb
        sudo dpkg -i asyncapi.deb

## Updating the specification

To update the specification, edit the `asyncapi.yaml` file and run:

```bash
asyncapi generate models golang api/asyncapi.yaml --packageName=domain -o domain && gofmt -w .
```
