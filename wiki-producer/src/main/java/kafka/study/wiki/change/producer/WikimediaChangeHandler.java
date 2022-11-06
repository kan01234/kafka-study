package kafka.study.wiki.change.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    private final Logger logger = LoggerFactory.getLogger(WikiChangerProducer.class.getName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onComment(String comment) throws Exception {
    }

    @Override
    public void onError(Throwable t) {
        logger.error("error", t);
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        // async send to topic
        kafkaProducer.send(new ProducerRecord<String,String>(topic, messageEvent.getData()));
    }

    @Override
    public void onOpen() throws Exception {
    }
    
}
