About
====================
Kafka demo

This is a code example that complements the material in the Kafka O'Reilly book. 


Requirements
====================
Requires Java 8 and Maven to run.


Build
====================
1. check out
2. mvn clean install


Run
====================
1. TConsumerString/TProducerString: string serialization
2. TConsumerCustom/TProducerCustom: custom serialization
3. TConsumerAvroSchema/TProducerAvroSchema: avro schema serialization
4. TConsumerAvroGeneral/TProducerGeneral: avro general serialization
5. TConsumerOffset: how to commit offset
6. TConsumerStandalone: how to use shutdownhook
7. TPartition: partitioner
8. TRebalance: reblancer


How to generate TCustomerSerializer.java
====================

```shell
java -jar %AVRO_TOOLS_JAR_PATH% compile schema TCustomerSchema.avsc java
```


How to get kafka-avro-serializer
====================
Modify maven settings.xml file, add:

```xml
<mirror>
      <id>confluent</id>
      <mirrorOf>confluent</mirrorOf>
      <name>Nexus public mirror</name>
      <url>http://packages.confluent.io/maven/</url>
</mirror>

<repository>
          <id>confluent</id>
          <url>http://packages.confluent.io/maven/</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
</repository>
```


How to get schema-registry
====================
https://www.confluent.io/download/

https://github.com/confluentinc/schema-registry


How to use schema-registry
====================
```shell
# List all subjects
$ curl -X GET -i http://localhost:8081/subjects

# Register a new version of a schema under the subject "TCustomerAvroSchema-key"
$ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data KEY_SCHEMA_DATA \
    http://localhost:8081/subjects/TCustomerAvroSchema-key/versions

# Register a new version of a schema under the subject "TCustomerAvroSchema-value"
$ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data VALUE_SCHEMA_DATA \
    http://localhost:8081/subjects/TCustomerAvroSchema-value/versions

# List all schema versions registered under the subject "TCustomerAvroSchema-value"
$ curl -X GET -i http://localhost:8081/subjects/TCustomerAvroSchema-value/versions

# Fetch version 1 of the schema registered under subject "TCustomerAvroSchema-value"
$ curl -X GET -i http://localhost:8081/subjects/TCustomerAvroSchema-value/versions/1

# Fetch the most recently registered schema under subject "TCustomerAvroSchema-value"
$ curl -X GET -i http://localhost:8081/subjects/TCustomerAvroSchema-value/versions/latest

# Delete subject
curl -X DELETE -i http://localhost:8081/subjects/TCustomerAvroSchema-key

# Update compatibility requirements globally
$ curl -X PUT -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}' \
    http://localhost:8081/config

# Update compatibility requirements under the subject "TCustomer"
$ curl -X PUT -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "BACKWARD"}' \
    http://localhost:8081/config/TCustomerAvroSchema-key

# Test compatibility of a schema with the latest schema under subject "TCustomer"
$ curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data KEY_SCHEMA_DATA \
    http://localhost:8081/compatibility/subjects/TCustomerAvroSchema-key/versions/latest
```
