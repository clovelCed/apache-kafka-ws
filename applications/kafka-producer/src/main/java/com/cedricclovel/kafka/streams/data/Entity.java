package com.cedricclovel.kafka.streams.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Entity {

    @JsonProperty("hashtags")
    public Hashtag[] hashtags;

    @JsonProperty("user_mentions")
    public UserMentions[] userMentions;
}
