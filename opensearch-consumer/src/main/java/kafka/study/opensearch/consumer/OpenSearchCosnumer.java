package kafka.study.opensearch.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchCosnumer {

  private static final Logger logger = LoggerFactory.getLogger(OpenSearchCosnumer.class.getName());

  public static void main(String[] args) throws IOException {
    // create opensearch client
    RestHighLevelClient opensearchClient = createOpenSearchClient();

    KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

    try (opensearchClient; kafkaConsumer) {
      // check index exists
      if (!opensearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
        // create index on opensearch
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        opensearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      }

      kafkaConsumer.subscribe(Arrays.asList("wikimedia.recentchange"));

      while(true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

        logger.info("received " + records.count() + "records");

        BulkRequest bulkRequest = new BulkRequest();
          
        for (ConsumerRecord<String, String> record: records) {
          // create bulk request
          bulkRequest = new BulkRequest();

          // make it idempotent
          String id = record.topic() + "-" + record.partition() + "-" + record.offset();

          try {
            IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
            bulkRequest.add(indexRequest);
            // IndexResponse indexResponse = opensearchCleint.index(indexRequest, RequestOptions.DEFAULT);
            // logger.info(indexResponse.getId());
          } catch (Exception e) {
          }
        }

        if (bulkRequest.numberOfActions() > 0) {
          // bulk request to opensearch
          opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

          // commit after batch is processed
          kafkaConsumer.commitSync();
        }
      }
    }

  }

  public static KafkaConsumer<String, String> createKafkaConsumer() {
    // create properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-group-1");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    return consumer;
  }

  public static RestHighLevelClient createOpenSearchClient() {
    String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
        // REST client without security
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
        // REST client with security
        String[] auth = userInfo.split(":");

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


    }

    return restHighLevelClient;
}
}
