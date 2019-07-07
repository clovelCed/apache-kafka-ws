package com.cedricclovel.kafka.streams;

import com.cedricclovel.kafka.data.TweetAvro;
import com.cedricclovel.kafka.data.TweetIdAvro;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class CleanTweet {

    private static final String EN = "en";

    private static final String FR = "fr";

    public static void main(String[] args) {
        new CleanTweet().run();
    }

    private void run() {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.<TweetIdAvro, TweetAvro>stream("twitter_source")
                .filter(this::textDoesNotContainsFrantzKafka)
                .filter((key, value) -> !value.getIsRetweet())
                .filter((key, value) -> (EN.equals(value.getLang()) || FR.equals(value.getLang())))
                .to("twitter_cleaned");

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), createKStreamProperties());

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private boolean textDoesNotContainsFrantzKafka(TweetIdAvro id, TweetAvro tweet) {
        return !(tweet.getText().toLowerCase().contains("franz")
                && tweet.getText().toLowerCase().contains("kafka"));
    }

    private Properties createKStreamProperties() {
        final Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "anonymized-app");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return properties;
    }
}
