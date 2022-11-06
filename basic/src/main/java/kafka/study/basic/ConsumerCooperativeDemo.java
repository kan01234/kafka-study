package kafka.study.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCooperativeDemo {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerCooperativeDemo.class.getName());

  public static void main(String[] args) {
    logger.info("start");
    // create properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group-1");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // get thread
    final Thread thread = Thread.currentThread();

    // shutdown task
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("run shutdown hook");
        consumer.wakeup();

        // join main thread to wait for finish
        try {
          thread.join();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    // subscribe topic
    consumer.subscribe(Arrays.asList("demo-java"));

    try {
      while (true) {
        // logger.info("polling");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
  
        for (ConsumerRecord<String, String> record: records) {
          logger.info("Key: " + record.key() + ", Value: " + record.value() + ", Partitions: " + record.partition() + ", offset: " + record.offset());
        }
  
      }
    } catch (WakeupException e) {
      // known excetpion for closing consumer
      logger.info("wakeup exception here");
    } catch (Exception e) {
      logger.error("unexpected exception", e);
    } finally {
      consumer.close();
      logger.info("conusmer closed");
    }
  }
}
