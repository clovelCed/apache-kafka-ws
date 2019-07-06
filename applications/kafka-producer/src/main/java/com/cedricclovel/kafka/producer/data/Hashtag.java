package com.cedricclovel.kafka.producer.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Hashtag {

    @JsonProperty("text")
    public String text;
}
