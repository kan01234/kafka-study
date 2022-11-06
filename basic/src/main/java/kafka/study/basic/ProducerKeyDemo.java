package kafka.study.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerKeyDemo {

  private static final Logger logger = LoggerFactory.getLogger(ProducerKeyDemo.class.getName());

  public static void main(String[] args) {
    logger.info("start");
    // create properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    
    for (int i = 0; i < 10; i ++) {

      //create producer record
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-java", "key-" + i, "value-" + i);

      producer.send(producerRecord, new Callback() {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          // successful send or exception
          if (exception == null) {
            logger.info(
              "Received metadata \n" +
              "Topic:" + metadata.topic() + "\n" +
              "Key:" + producerRecord.key() + "\n" +
              "Partition: " + metadata.partition() + "\n" +
              "Offset: " + metadata.offset() + "\n" +
              "Timestamp" + metadata.timestamp() + "\n"
            );
          } else {
            logger.error("Error while send msg", exception);
          }
        }
      });
    }

    producer.flush();
    producer.close();

  }
}
