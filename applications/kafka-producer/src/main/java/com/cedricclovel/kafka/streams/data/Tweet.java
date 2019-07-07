package com.cedricclovel.kafka.streams.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {

    @JsonProperty("id")
    public Long id;

    @JsonProperty("text")
    public String text;

    @JsonProperty("source")
    public String source;

    @JsonProperty("created_at")
    public String createdAt;

    @JsonProperty("retweeted")
    public Boolean retweeted;

    @JsonProperty("retweet_count")
    public Integer retweetCount;

    @JsonProperty("entities")
    public Entity entities;

    @JsonProperty("user")
    public User user;

    @JsonProperty("retweeted_status")
    public Tweet retweetedStatus ;

    @JsonProperty("lang")
    public String lang;
}
