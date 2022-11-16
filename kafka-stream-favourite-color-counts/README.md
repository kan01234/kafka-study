###
create topic
```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic color-input
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic user-and-color-input --config cleanup.policy=compact
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic color-count-output --config cleanup.policy=compact
```

produce color
```
kafka-console-producer --bootstrap-server localhost:9092 --topic color-input

stephane,blue
john,green
stephane,red
alice,red
```

check output from java application
```
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic user-color-input \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic color-count-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```