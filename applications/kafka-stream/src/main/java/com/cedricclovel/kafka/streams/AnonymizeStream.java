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

public class AnonymizeStream {

    public static void main(String[] args) {
        new AnonymizeStream().run();
    }

    private void run() {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.<TweetIdAvro, TweetAvro>stream("twitter_source")
                .filter(this::textDoesNotContainsFrantzKafka)
                .mapValues(tweet -> {
                    tweet.getUser().setName(anonymizeLastName(tweet.getUser().getName()));
                    tweet.getEntity().getUserMentions().forEach(userMention -> userMention.setName(anonymizeLastName(userMention.getName())));
                    return tweet;
                }).to("twitter_anonymized");

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), createKStreamProperties());

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    // Do better for anonymize
    private String anonymizeLastName(String fullName) {

        final String[] names = fullName.split(" ");

        final StringBuilder lastNameAnonymized = new StringBuilder(names[0]).append(" ");

        if (names.length > 1) {

            final int lastNameCharactersNumber = names[1].length();

            for (int i = 0; i < lastNameCharactersNumber; i++) {
                lastNameAnonymized.append("X");
            }
        }

        return lastNameAnonymized.toString();
    }

    private boolean textDoesNotContainsFrantzKafka(TweetIdAvro id, TweetAvro tweet) {
        return !(tweet.getText().toLowerCase().contains("frantz")
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
