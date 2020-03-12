# avlizer - Avro Serializer for Erlang

`avlizer` is built on top of [erlavro](https://github.com/klarna/erlavro)
The design goal of this application is to provide APIs for users to serialize
or deserialize data with minimal overhead of schema management.

# Integrations

## Confluent Schema Registry

[Confluent schema registry](https://github.com/confluentinc/schema-registry)
provides REST APIs for schema registration and lookups.

### Config
Make sure schema registry URL is present in `sys.config` as below

```
{avlizer, [{avlizer_confluent, #{schema_registry_url => URL}}]}
```

Or set os env variable: 

`AVLIZER_CONFLUENT_SCHEMAREGISTRY_URL`

### Authentication support
avlizer currently supports `Basic` authentication mechanism, to use it make sure `schema_registry_auth` tuple `{Mechanism, File}`, is present in `sys.config`, where File is the path to a text file which contains two lines, first line for username and second line for password

```
{avlizer, [{avlizer_confluent, #{
    schema_registry_url => URL, 
    schema_registry_auth => {basic, File}}
}]}
```

Or set authorization env variables:

`AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_MECHANISM`

`AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_USERNAME`

`AVLIZER_CONFLUENT_SCHEMAREGISTRY_AUTH_PASSWORD`
