package kafka.study.stream.wordcounts;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-starter-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // stream from kafka
        KStream<String, String> wordCountInputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = wordCountInputStream
            // to lower case
            .mapValues(value -> value.toLowerCase())
            // flatmap values split by space
            .flatMapValues(value -> Arrays.asList(value.split((" "))))
            // select key to apply a key
            .selectKey((key, value) -> value)
            // group by key
            .groupByKey()
            .count();

        wordCounts.toStream().to("word-count-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        System.out.println(streams.toString());

        // close gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
