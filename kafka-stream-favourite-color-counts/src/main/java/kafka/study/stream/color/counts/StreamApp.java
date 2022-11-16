package kafka.study.stream.color.counts;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class StreamApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "color-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // stream from kafka
        KStream<String, String> colorTextInputStream = builder.stream("color-input");
        Set<String> availableColors = new HashSet<String>(Arrays.asList("red", "green", "blue"));

        KStream<String, String> userColorInputStream = colorTextInputStream
            // filter
            .filter((k, v) -> v.contains(","))
            // select key to apply a key
            .selectKey((key, value) -> value.split(",")[0])
            // to lower case
            .mapValues(value -> value.toLowerCase())
            // flatmap values split by ","
            .mapValues(value -> value.split(",")[1])
            // filter color
            .filter((k, v) -> availableColors.contains(v));
            

            userColorInputStream.to("user-color-input");

        KTable<String, String> userColorTable = builder.table("user-color-input");

        KTable<String, Long> colorCountTable = userColorTable
            .groupBy((k, v) -> new KeyValue(v, v))
            .count();

        colorCountTable.toStream().to("color-count-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        System.out.println(streams.toString());

        // close gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
