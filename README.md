# dynamodb-client

A library simplifying working with dynamodb client in aws sdk v3.

## Integration tests

The integration tests are not automated or part of the build (yet...). To run them locally, make sure to have `docker` installed and then:

`docker run -v $(pwd)/data:/data -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /data`

`npm run integration-tests`

To avoid having the data folder be created in different places, creating a folder like ~/dynamo and a script in it like below, and then starting the db by running the script will keep everything within that folder.

``` bash
#!/bin/sh
docker run -v $(pwd)/data:/data -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -dbPath /data
```
