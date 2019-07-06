package com.cedricclovel.kafka.producer;

import avro.shaded.com.google.common.collect.Lists;
import com.cedricclovel.kafka.producer.data.Tweet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringSerializer;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOGGER = Logger.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");

    private static final String CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");

    private static final String TOKEN = System.getenv("TWITTER_TOKEN");

    private static final String TOKEN_SECRET = System.getenv("TWITTER_TOKEN_SECRET");

    private static final List<String> TERMS = Lists.newArrayList("kafka", "confluent", "bigdata", "java", "scala", "datascience");

    private TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {

        // Twitter initialisation
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        final Client twitterClient = this.createTwitterClient(msgQueue);

        twitterClient.connect();

        // Create Kafka Producer
        final KafkaProducer<String, String> kafkaProducer = this.createKafkaProducer();

        final ObjectMapper objectMapper = new ObjectMapper();

        while (!twitterClient.isDone()) {

            String msg;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

                if (msg != null) {

                    final Tweet tweetPojo = objectMapper.readValue(msg, Tweet.class);

                    System.out.println(tweetPojo);

                }


            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
        }

        LOGGER.info("End of application");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            LOGGER.info("Closing twitter client");
            LOGGER.info("Closing kafka producer");
            twitterClient.stop();
            kafkaProducer.close();
        }));
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(TERMS);

        final Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        final ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(this.createKafkaProducerProperties());
    }

    private Properties createKafkaProducerProperties() {

        final Properties properties = new Properties();

        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }
}
