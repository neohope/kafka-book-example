About
====================
kkdemo
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
3. TConsumerAvro/TProducerAvro: avro serialization


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
