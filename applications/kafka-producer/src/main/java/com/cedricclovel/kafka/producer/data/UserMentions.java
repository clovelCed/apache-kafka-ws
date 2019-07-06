package com.cedricclovel.kafka.producer.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UserMentions {

    @JsonProperty("id")
    public Long id;

    @JsonProperty("screen_name")
    public String screenName;

    @JsonProperty("name")
    public String name;
}
