package com.cedricclovel.kafka.streams.data;

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
