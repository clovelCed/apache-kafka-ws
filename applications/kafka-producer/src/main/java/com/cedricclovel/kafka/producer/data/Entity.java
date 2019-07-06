package com.cedricclovel.kafka.producer.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Entity {

    @JsonProperty("hashtags")
    public Hashtag[] hashtags;

    @JsonProperty("user_mentions")
    public UserMentions[] userMentions;
}
