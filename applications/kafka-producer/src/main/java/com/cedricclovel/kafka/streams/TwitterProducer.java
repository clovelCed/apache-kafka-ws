package com.cedricclovel.kafka.streams;

import avro.shaded.com.google.common.collect.Lists;
import com.cedricclovel.kafka.data.*;
import com.cedricclovel.kafka.streams.data.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class TwitterProducer {

    private static final Logger LOGGER = Logger.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");

    private static final String CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");

    private static final String TOKEN = System.getenv("TWITTER_TOKEN");

    private static final String TOKEN_SECRET = System.getenv("TWITTER_TOKEN_SECRET");

    private static final List<String> TERMS = Lists.newArrayList("apache kafka", "apachekafka", "kafka", "confluent", "bigdata", "hadoop", "apache spark", "apachespark", "data science", "datascience", "machine learning", "deep learning", "dataengineer", "data engineer");

    private TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {

        final ObjectMapper objectMapper = new ObjectMapper();

        // Twitter initialisation
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        final Client twitterClient = this.createTwitterClient(msgQueue);
        twitterClient.connect();

        // Create Kafka Producer
        final KafkaProducer kafkaProducer = this.createKafkaProducer();

        while (!twitterClient.isDone()) {

            String msg;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

                if (msg != null) {

                    final Tweet tweet = objectMapper.readValue(msg, Tweet.class);

                    final TweetIdAvro tweetIdAvro = mapToTweetIdAvro(tweet.id);
                    final TweetAvro tweetAvro = mapToTweetAvro(tweet);

                    final ProducerRecord<TweetIdAvro, TweetAvro> tweetToPush = new ProducerRecord<>("twitter_source", tweetIdAvro, tweetAvro);

                    kafkaProducer.send(tweetToPush, (recordMetadata, e) -> {
                        if (e != null) {
                            // Do something better
                            LOGGER.error(e);
                        }
                    });
                }
            } catch (InterruptedException | IOException e) {
                LOGGER.error(e);
                twitterClient.stop();
            }
        }

        LOGGER.info("End of application");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.stop();
            kafkaProducer.close();
        }));
    }

    private KafkaProducer createKafkaProducer() {

        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://0.0.0.0:8081");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private TweetAvro mapToTweetAvro(Tweet tweet) {

        return TweetAvro.newBuilder()
                .setId(tweet.id)
                .setText(tweet.text)
                .setLang(tweet.lang)
                .setCreatedAt(tweet.createdAt)
                .setSource(tweet.source)
                .setRetweeted(tweet.retweeted)
                .setRetweetCount(tweet.retweetCount)
                .setUser(mapToUserAvro(tweet.user))
                .setEntity(mapToEntityAvro(tweet.entities))
                .setIsRetweet(tweet.retweetedStatus != null)
                .build();
    }

    private EntityAvro mapToEntityAvro(Entity entity) {

        final List<HashTagAvro> hashTags = Arrays.stream(entity.hashtags)
                .map(this::mapToHashTagAvro)
                .collect(toList());

        final List<UserMentionAvro> userMentions = Arrays.stream(entity.userMentions)
                .map(this::mapToUserMentionAvro)
                .collect(toList());

        return EntityAvro.newBuilder()
                .setUserMentions(userMentions)
                .setHastags(hashTags)
                .build();
    }

    private TweetIdAvro mapToTweetIdAvro(Long id) {
        return TweetIdAvro.newBuilder().setId(id).build();
    }

    private HashTagAvro mapToHashTagAvro(Hashtag hashtag) {
        return HashTagAvro.newBuilder()
                .setText(hashtag.text)
                .build();
    }

    private UserMentionAvro mapToUserMentionAvro(UserMentions userMention) {
        return UserMentionAvro.newBuilder()
                .setId(userMention.id)
                .setName(userMention.name)
                .setScreenName(userMention.screenName)
                .build();
    }

    private UserAvro mapToUserAvro(User user) {

        return UserAvro.newBuilder()
                .setId(user.id)
                .setName(user.name)
                .setScreenName(user.screenName)
                .setFollowersCount(user.followersCount)
                .setFriendsCount(user.friendsCount)
                .setStatusesCount(user.statusesCount)
                .build();
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
}
